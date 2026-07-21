//! Agent of Empires cron worker.
//!
//! Self-scheduling: the host does not fire timers for it. The worker reads its
//! `jobs` setting, seeds each job's cursor to "now" on startup (skip-missed), and
//! on each tick asks the pure planner which jobs are due, then creates a session
//! per fire. It wakes early when the host pushes `plugin.settings.changed` and
//! re-reads the jobs. stdin EOF (the host tearing the worker down) exits cleanly.

mod host;
mod rpc;
mod schedule;

use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use chrono::Local;

use rpc::Peer;
use schedule::{plan_tick, restore_failed_cursors, Cursors};

/// Poll cadence between settings-change wakeups. Cron granularity is one minute,
/// so a sub-minute tick guarantees every occurrence lands in some window; a
/// bounded sleep also caps clock-drift from a long suspend.
const TICK: Duration = Duration::from_secs(30);

fn main() {
    // Identity is informational; the host authorizes every call by the worker's
    // connection, not by anything the worker sends.
    let plugin_id = std::env::var("AOE_PLUGIN_ID").unwrap_or_default();
    eprintln!("plugin-cron worker starting (plugin_id={plugin_id})");

    let (peer, notify_rx) = Peer::start();

    // `jobs_stale` records a failed fetch so the tick loop retries it, rather
    // than running with zero jobs until the next settings-change push.
    let (mut jobs, mut jobs_stale) = match host::fetch_jobs(peer.as_ref()) {
        Ok(jobs) => (jobs, false),
        Err(e) => {
            eprintln!("initial config.get failed: {}", e.message);
            (Vec::new(), true)
        }
    };

    // Skip-missed: the first plan_tick seeds every cursor to now and fires
    // nothing, so occurrences that elapsed before startup are discarded.
    let (_, mut cursors): (_, Cursors<Local>) = plan_tick(&jobs, &Cursors::new(), &Local::now());

    loop {
        match notify_rx.recv_timeout(TICK) {
            Ok(notification) => {
                if notification.method.ends_with("settings.changed") {
                    match host::fetch_jobs(peer.as_ref()) {
                        Ok(fresh) => {
                            jobs = fresh;
                            jobs_stale = false;
                        }
                        Err(e) => {
                            eprintln!("config.get after settings change failed: {}", e.message);
                            jobs_stale = true;
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                eprintln!("host closed stdin; worker exiting");
                break;
            }
        }

        // Self-heal a failed initial (or post-change) fetch: retry once per tick
        // while stale, so a transient error does not leave the worker jobless
        // until the next settings edit.
        if jobs_stale {
            match host::fetch_jobs(peer.as_ref()) {
                Ok(fresh) => {
                    jobs = fresh;
                    jobs_stale = false;
                }
                Err(e) => eprintln!("config.get retry failed: {}", e.message),
            }
        }

        let now = Local::now();
        let (fires, next) = plan_tick(&jobs, &cursors, &now);
        if fires.is_empty() {
            cursors = next;
            continue;
        }

        // Minute-granular fire key: stable across the few seconds between the
        // occurrence and this tick, so the host idempotency key does not drift.
        let fire_key = now.format("%Y-%m-%dT%H:%M").to_string();
        let mut failed = Vec::new();
        for id in fires {
            let Some(job) = jobs.iter().find(|j| j.id == id) else {
                continue;
            };
            match host::create_session(peer.as_ref(), job, &fire_key) {
                Ok(outcome) => eprintln!(
                    "fired job {} ({}) -> session {} (created={})",
                    job.id, job.name, outcome.session_id, outcome.created
                ),
                Err(e) => {
                    eprintln!(
                        "job {} ({}) create failed [{}]: {}",
                        job.id, job.name, e.code, e.message
                    );
                    failed.push(id);
                }
            }
        }
        // Commit advanced cursors, but roll back any job whose create failed so
        // the occurrence retries on the next tick instead of being dropped.
        cursors = restore_failed_cursors(&cursors, next, &failed);
    }
}
