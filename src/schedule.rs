//! Pure scheduling core, ported from the reverted in-core scheduler
//! (agent-of-empires #2887). A [`Job`] fires a preset prompt against an agent on
//! a cron schedule. This module is deliberately free of I/O and the host RPC
//! layer: it decides *which* jobs are due, and `main` turns a fire into a
//! `sessions.create` call.
//!
//! Cron expressions are interpreted in the timezone of the timestamps passed to
//! [`next_due`] / [`plan_tick`]; the worker passes the daemon host's local time.
//! Standard 5-field expressions (min hour dom mon dow).

use serde::Deserialize;

/// Group scheduled sessions land in by default, so they do not muddle with
/// interactive ones.
pub const DEFAULT_GROUP: &str = "Scheduled";

fn default_true() -> bool {
    true
}

fn default_group() -> String {
    DEFAULT_GROUP.to_string()
}

/// One scheduled job, as stored in the plugin's `jobs` object-list setting and
/// returned (as JSON) by the `config.get` host RPC. The `id` is the host
/// generated stable item id (`item_id_key`), stable across edits and reorders,
/// so the runtime cursor can track a job across edits.
#[derive(Debug, Clone, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    /// Cron expression (5-field), interpreted in host-local time.
    pub schedule: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// ACP agent id (from the host's `acp.agents` option source).
    pub agent_id: String,
    /// Optional model id; empty or absent means the agent default.
    #[serde(default)]
    pub model_id: Option<String>,
    /// Optional approval-mode id; empty or absent means the agent default. The
    /// host classifies its security level, and an unattended mode needs the
    /// `session.unattended` grant plus a trusted repository.
    #[serde(default)]
    pub mode_id: Option<String>,
    /// Project working paths. Empty means no project: the host runs a scratch
    /// session. The first path is the primary (trust-checked) repo; any extra
    /// paths become additional repos of a multi-repo session.
    #[serde(default)]
    pub project_paths: Vec<String>,
    /// Run the session inside the host sandbox (a container). Off by default;
    /// the host uses its configured sandbox image and fails the create if no
    /// container runtime is available.
    #[serde(default)]
    pub sandbox: bool,
    /// Prompt delivered to the session's first turn.
    pub prompt: String,
    #[serde(default = "default_group")]
    pub group: String,
}

/// The occurrence to fire if `schedule` has a match in the half-open window
/// `(last_check, now]`, else `None`. Returns at most one occurrence (the
/// earliest in the window), so a delayed tick that spans several matches fires
/// only once rather than bursting. Occurrences are interpreted in the timezone
/// of the supplied timestamps; the scheduler passes host-local time.
///
/// This is the pure core of the skip-missed policy: seeding `last_check` to
/// "now" at startup means occurrences that elapsed while the worker was down are
/// never in a future window and so are skipped.
pub fn next_due<Tz: chrono::TimeZone>(
    schedule: &str,
    last_check: &chrono::DateTime<Tz>,
    now: &chrono::DateTime<Tz>,
) -> Option<chrono::DateTime<Tz>> {
    let cron = croner::Cron::new(schedule).parse().ok()?;
    let next = cron.find_next_occurrence(last_check, false).ok()?;
    if next <= *now {
        Some(next)
    } else {
        None
    }
}

/// Per-job runtime cursor. Held in memory only (never persisted): the
/// skip-missed policy relies on cursors seeding to "now" every startup so
/// downtime is discarded.
#[derive(Debug, Clone)]
pub struct JobCursor<Tz: chrono::TimeZone> {
    /// `(enabled, schedule)` when the cursor was last seeded. A change means the
    /// job was enabled/disabled or rescheduled, so we reseed rather than fire
    /// retroactively.
    fingerprint: (bool, String),
    /// Upper bound of the last evaluated window. Advances only when the job
    /// fires, so an occurrence is caught even if it lands between ticks.
    last_check: chrono::DateTime<Tz>,
}

/// Cursors keyed by job id.
pub type Cursors<Tz> = std::collections::HashMap<String, JobCursor<Tz>>;

/// Pure scheduler step: given the current jobs, the prior cursors, and `now`,
/// return the ids to fire and the updated cursor map.
///
/// Rules (the debate's converged semantics):
/// - A job absent from `prev` is seeded to `now` and does NOT fire (new job /
///   fresh worker skips past occurrences).
/// - A job whose `(enabled, schedule)` fingerprint changed is reseeded to `now`
///   and does NOT fire (an edit never retroactively fires).
/// - An unchanged enabled job fires at most once if an occurrence falls in
///   `(last_check, now]`; on fire its cursor advances to `now`.
/// - Disabled jobs never fire.
/// - Cursors for jobs no longer present are dropped (only current jobs are
///   carried forward).
pub fn plan_tick<Tz: chrono::TimeZone>(
    jobs: &[Job],
    prev: &Cursors<Tz>,
    now: &chrono::DateTime<Tz>,
) -> (Vec<String>, Cursors<Tz>) {
    let mut fires = Vec::new();
    let mut next: Cursors<Tz> = std::collections::HashMap::with_capacity(jobs.len());

    for job in jobs {
        let fp = (job.enabled, job.schedule.clone());
        let cursor = match prev.get(&job.id) {
            // New job or changed fingerprint: reseed to now, do not fire.
            None => JobCursor {
                fingerprint: fp,
                last_check: now.clone(),
            },
            Some(c) if c.fingerprint != fp => JobCursor {
                fingerprint: fp,
                last_check: now.clone(),
            },
            // Unchanged: evaluate the window.
            Some(c) => {
                let mut last_check = c.last_check.clone();
                if job.enabled && next_due(&job.schedule, &c.last_check, now).is_some() {
                    fires.push(job.id.clone());
                    last_check = now.clone();
                }
                JobCursor {
                    fingerprint: fp,
                    last_check,
                }
            }
        };
        next.insert(job.id.clone(), cursor);
    }

    (fires, next)
}

/// Roll back the cursors of jobs whose session creation failed, restoring their
/// pre-tick state so the occurrence stays in the next window and retries. Jobs
/// that fired successfully (or did not fire) keep their advanced cursor.
///
/// `plan_tick` advances a fired job's cursor optimistically; the scheduler
/// calls this after attempting each create so a transient `sessions.create`
/// failure does not silently drop the occurrence. A retry is safe: the host
/// idempotency key (`job_id:fire_minute`) collapses a duplicate if the failed
/// create had in fact taken effect.
pub fn restore_failed_cursors<Tz: chrono::TimeZone>(
    prev: &Cursors<Tz>,
    mut next: Cursors<Tz>,
    failed: &[String],
) -> Cursors<Tz> {
    for id in failed {
        if let Some(prev_cursor) = prev.get(id) {
            next.insert(id.clone(), prev_cursor.clone());
        }
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: &str, schedule: &str) -> Job {
        Job {
            id: id.to_string(),
            name: "daily pr check".to_string(),
            schedule: schedule.to_string(),
            enabled: true,
            agent_id: "claude".to_string(),
            model_id: None,
            mode_id: None,
            project_paths: vec!["/tmp/project".to_string()],
            sandbox: false,
            prompt: "check open PRs".to_string(),
            group: DEFAULT_GROUP.to_string(),
        }
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> chrono::DateTime<chrono::Utc> {
        use chrono::TimeZone;
        chrono::Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn deserializes_job_with_defaults() {
        // model_id / mode_id / group / enabled are all optional in the stored
        // JSON; their defaults must apply.
        let v = serde_json::json!({
            "id": "abc",
            "name": "nightly",
            "schedule": "0 3 * * *",
            "agent_id": "claude",
            "project_paths": ["/repo"],
            "prompt": "run nightly"
        });
        let j: Job = serde_json::from_value(v).unwrap();
        assert!(j.enabled);
        assert_eq!(j.project_paths, vec!["/repo".to_string()]);
        assert_eq!(j.group, "Scheduled");
        assert_eq!(j.model_id, None);
        assert_eq!(j.mode_id, None);
    }

    #[test]
    fn next_due_fires_when_occurrence_in_window() {
        // daily at 08:00; window (07:00, 08:30] contains 08:00.
        let got = next_due(
            "0 8 * * *",
            &utc(2026, 7, 16, 7, 0),
            &utc(2026, 7, 16, 8, 30),
        );
        assert_eq!(got, Some(utc(2026, 7, 16, 8, 0)));
    }

    #[test]
    fn next_due_none_when_no_occurrence_in_window() {
        // window (08:30, 09:00] has no daily-08:00 occurrence.
        let got = next_due(
            "0 8 * * *",
            &utc(2026, 7, 16, 8, 30),
            &utc(2026, 7, 16, 9, 0),
        );
        assert_eq!(got, None);
    }

    #[test]
    fn next_due_collapses_multiple_occurrences_to_one() {
        // every 10 min; window (08:00, 08:35] has 08:10/08:20/08:30 -> only 08:10.
        let got = next_due(
            "*/10 * * * *",
            &utc(2026, 7, 16, 8, 0),
            &utc(2026, 7, 16, 8, 35),
        );
        assert_eq!(got, Some(utc(2026, 7, 16, 8, 10)));
    }

    #[test]
    fn next_due_includes_occurrence_exactly_at_now() {
        let got = next_due(
            "0 8 * * *",
            &utc(2026, 7, 16, 7, 59),
            &utc(2026, 7, 16, 8, 0),
        );
        assert_eq!(got, Some(utc(2026, 7, 16, 8, 0)));
    }

    #[test]
    fn next_due_invalid_cron_is_none() {
        assert_eq!(
            next_due("nonsense", &utc(2026, 7, 16, 7, 0), &utc(2026, 7, 16, 9, 0)),
            None
        );
    }

    #[test]
    fn plan_tick_seeds_new_job_without_firing() {
        let jobs = vec![job("a", "0 8 * * *")];
        let (fires, cursors) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 9, 0));
        assert!(
            fires.is_empty(),
            "a fresh job must not fire on its seed tick"
        );
        assert!(cursors.contains_key("a"));
    }

    #[test]
    fn plan_tick_fires_once_when_due() {
        let jobs = vec![job("a", "0 8 * * *")];
        let (_, c0) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 7, 0));
        let (fires, c1) = plan_tick(&jobs, &c0, &utc(2026, 7, 16, 8, 30));
        assert_eq!(fires, vec!["a".to_string()]);
        // Next tick in the same day does not refire.
        let (fires2, _) = plan_tick(&jobs, &c1, &utc(2026, 7, 16, 8, 31));
        assert!(fires2.is_empty());
    }

    #[test]
    fn plan_tick_skips_missed_occurrences_after_reseed() {
        // Seed at 09:00 (after the 08:00 occurrence). It must never fire for the
        // already-elapsed 08:00: this is the worker-was-down case.
        let jobs = vec![job("a", "0 8 * * *")];
        let (_, c0) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 9, 0));
        let (fires, _) = plan_tick(&jobs, &c0, &utc(2026, 7, 16, 9, 30));
        assert!(fires.is_empty());
    }

    #[test]
    fn plan_tick_disabled_job_never_fires() {
        let mut j = job("a", "0 8 * * *");
        j.enabled = false;
        let jobs = vec![j];
        let (_, c0) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 7, 0));
        let (fires, _) = plan_tick(&jobs, &c0, &utc(2026, 7, 16, 8, 30));
        assert!(fires.is_empty());
    }

    #[test]
    fn plan_tick_reenable_does_not_catch_up() {
        // Enabled then disabled then re-enabled around the occurrence: no catch-up.
        let mut disabled = job("a", "0 8 * * *");
        disabled.enabled = false;
        let enabled = job("a", "0 8 * * *");
        let (_, c0) = plan_tick(
            std::slice::from_ref(&enabled),
            &Cursors::new(),
            &utc(2026, 7, 16, 7, 0),
        );
        // Disable before the occurrence (fingerprint change reseeds).
        let (_, c1) = plan_tick(
            std::slice::from_ref(&disabled),
            &c0,
            &utc(2026, 7, 16, 7, 30),
        );
        // Re-enable after the occurrence: fingerprint change reseeds to 08:30, no fire.
        let (fires, _) = plan_tick(
            std::slice::from_ref(&enabled),
            &c1,
            &utc(2026, 7, 16, 8, 30),
        );
        assert!(fires.is_empty());
    }

    #[test]
    fn restored_cursor_refires_on_next_tick() {
        // A fired job whose create failed is rolled back, so the same
        // occurrence is still in the window on the next tick and fires again.
        let jobs = vec![job("a", "0 8 * * *")];
        let (_, c0) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 7, 0));
        let (fires, c1) = plan_tick(&jobs, &c0, &utc(2026, 7, 16, 8, 30));
        assert_eq!(fires, vec!["a".to_string()]);
        // Simulate the create failing for job "a".
        let c1 = restore_failed_cursors(&c0, c1, &fires);
        let (fires2, _) = plan_tick(&jobs, &c1, &utc(2026, 7, 16, 8, 31));
        assert_eq!(fires2, vec!["a".to_string()], "a failed fire must retry");
    }

    #[test]
    fn successful_fire_is_not_rolled_back() {
        // With no failures, the advanced cursor stands and the job does not
        // refire (the empty failed-list is a no-op).
        let jobs = vec![job("a", "0 8 * * *")];
        let (_, c0) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 7, 0));
        let (fires, c1) = plan_tick(&jobs, &c0, &utc(2026, 7, 16, 8, 30));
        assert_eq!(fires, vec!["a".to_string()]);
        let c1 = restore_failed_cursors(&c0, c1, &[]);
        let (fires2, _) = plan_tick(&jobs, &c1, &utc(2026, 7, 16, 8, 31));
        assert!(fires2.is_empty(), "a successful fire must not refire");
    }

    #[test]
    fn plan_tick_drops_removed_job_cursor() {
        let jobs = vec![job("a", "0 8 * * *")];
        let (_, c0) = plan_tick(&jobs, &Cursors::new(), &utc(2026, 7, 16, 7, 0));
        assert!(c0.contains_key("a"));
        let (_, c1) = plan_tick(&[], &c0, &utc(2026, 7, 16, 7, 30));
        assert!(!c1.contains_key("a"), "removed job cursor must be dropped");
    }
}
