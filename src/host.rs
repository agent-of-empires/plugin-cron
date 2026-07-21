//! Typed wrappers over the host RPCs the scheduler uses.

use serde_json::{json, Value};

use crate::rpc::{Caller, RpcError};
use crate::schedule::Job;

/// Read the plugin's `jobs` setting. An unset value (no jobs configured yet)
/// comes back as JSON null, which maps to an empty list.
pub fn fetch_jobs(caller: &impl Caller) -> Result<Vec<Job>, RpcError> {
    let response = caller.call("config.get", json!({ "key": "jobs" }))?;
    let value = response.get("value").cloned().unwrap_or(Value::Null);
    if value.is_null() {
        return Ok(Vec::new());
    }
    serde_json::from_value(value).map_err(|e| RpcError {
        code: -32603,
        message: format!("could not parse the jobs setting: {e}"),
    })
}

/// The result of a create: whether the host actually spawned a session or
/// replayed an existing one for the same idempotency key.
pub struct CreateOutcome {
    pub session_id: String,
    pub created: bool,
}

/// Create a structured session for a due job and deliver its prompt as the first
/// turn. The idempotency key is `job_id:fire_key`, so a retry after an ambiguous
/// response replays the existing session instead of spawning a second one.
pub fn create_session(
    caller: &impl Caller,
    job: &Job,
    fire_key: &str,
) -> Result<CreateOutcome, RpcError> {
    let mut params = json!({
        "agent_id": job.agent_id,
        "title": job.name,
        "group": job.group,
        "initial_turn": { "text": job.prompt },
        "idempotency_key": format!("{}:{}", job.id, fire_key),
    });
    // Map the selected projects onto the host contract: first is the primary
    // (trust-checked) repo, the rest are extra repos. No projects -> omit
    // project_path entirely so the host provisions a scratch session.
    let projects: Vec<&str> = job
        .project_paths
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();
    if let Some((primary, extras)) = projects.split_first() {
        params["project_path"] = json!(primary);
        if !extras.is_empty() {
            params["extra_project_paths"] = json!(extras);
        }
    }
    // Opt into the host sandbox; omit the key when off so the default stands.
    if job.sandbox {
        params["sandbox"] = json!(true);
    }
    if let Some(model) = non_empty(&job.model_id) {
        params["model_id"] = json!(model);
    }
    if let Some(mode) = non_empty(&job.mode_id) {
        params["mode_id"] = json!(mode);
    }

    let response = caller.call("sessions.create", params)?;
    // A response without a usable session_id is a host-contract violation, not a
    // success: surface it rather than reporting a "fired" job with an empty id.
    let session_id = response
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RpcError {
            code: -32603,
            message: "sessions.create response missing session_id".to_string(),
        })?
        .to_string();
    Ok(CreateOutcome {
        session_id,
        created: response
            .get("created")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// Treat an absent or blank optional string as "not set".
fn non_empty(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// A `Caller` that records the last request and returns a canned response.
    struct Mock {
        response: Value,
        last: RefCell<Option<(String, Value)>>,
    }

    impl Mock {
        fn new(response: Value) -> Self {
            Mock {
                response,
                last: RefCell::new(None),
            }
        }
    }

    impl Caller for Mock {
        fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
            *self.last.borrow_mut() = Some((method.to_string(), params));
            Ok(self.response.clone())
        }
    }

    fn job() -> Job {
        Job {
            id: "job-1".to_string(),
            name: "nightly".to_string(),
            schedule: "0 3 * * *".to_string(),
            enabled: true,
            agent_id: "claude".to_string(),
            model_id: None,
            mode_id: None,
            project_paths: vec!["/repo".to_string()],
            sandbox: false,
            prompt: "run nightly".to_string(),
            group: "Scheduled".to_string(),
        }
    }

    #[test]
    fn fetch_jobs_null_is_empty() {
        let mock = Mock::new(json!({ "value": null, "revision": 3 }));
        assert!(fetch_jobs(&mock).unwrap().is_empty());
    }

    #[test]
    fn fetch_jobs_parses_list() {
        let mock = Mock::new(json!({
            "value": [{
                "id": "x", "name": "n", "schedule": "0 8 * * *",
                "agent_id": "claude", "project_paths": ["/r"], "prompt": "p"
            }],
            "revision": 1
        }));
        let jobs = fetch_jobs(&mock).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "x");
        assert_eq!(jobs[0].group, "Scheduled"); // default applied
    }

    #[test]
    fn create_session_builds_minimal_params() {
        let mock = Mock::new(json!({ "session_id": "s1", "created": true }));
        let out = create_session(&mock, &job(), "2026-07-20T03:00").unwrap();
        assert_eq!(out.session_id, "s1");
        assert!(out.created);

        let (method, params) = mock.last.borrow().clone().unwrap();
        assert_eq!(method, "sessions.create");
        assert_eq!(params["agent_id"], "claude");
        assert_eq!(params["project_path"], "/repo");
        assert_eq!(params["group"], "Scheduled");
        assert_eq!(params["initial_turn"]["text"], "run nightly");
        assert_eq!(params["idempotency_key"], "job-1:2026-07-20T03:00");
        // Unset model / mode must be omitted, not sent as empty strings.
        assert!(params.get("model_id").is_none());
        assert!(params.get("mode_id").is_none());
        // A single project sends no extra repos.
        assert!(params.get("extra_project_paths").is_none());
        // Sandbox off -> key omitted, not sent as false.
        assert!(params.get("sandbox").is_none());
    }

    #[test]
    fn create_session_sets_sandbox_when_enabled() {
        let mock = Mock::new(json!({ "session_id": "s1", "created": true }));
        let mut j = job();
        j.sandbox = true;
        create_session(&mock, &j, "k").unwrap();
        let (_, params) = mock.last.borrow().clone().unwrap();
        assert_eq!(params["sandbox"], json!(true));
    }

    #[test]
    fn create_session_omits_project_for_scratch() {
        let mock = Mock::new(json!({ "session_id": "s1", "created": true }));
        let mut j = job();
        j.project_paths = Vec::new();
        create_session(&mock, &j, "k").unwrap();
        let (_, params) = mock.last.borrow().clone().unwrap();
        // No project selected -> omit project_path so the host runs a scratch
        // session; never send an empty string.
        assert!(params.get("project_path").is_none());
        assert!(params.get("extra_project_paths").is_none());
    }

    #[test]
    fn create_session_maps_multi_repo() {
        let mock = Mock::new(json!({ "session_id": "s1", "created": true }));
        let mut j = job();
        j.project_paths = vec![
            "/repo/app".to_string(),
            "  ".to_string(), // blank entries are dropped
            "/repo/lib".to_string(),
            "/repo/proto".to_string(),
        ];
        create_session(&mock, &j, "k").unwrap();
        let (_, params) = mock.last.borrow().clone().unwrap();
        assert_eq!(params["project_path"], "/repo/app");
        assert_eq!(
            params["extra_project_paths"],
            json!(["/repo/lib", "/repo/proto"])
        );
    }

    #[test]
    fn create_session_rejects_response_without_session_id() {
        // A success-shaped response lacking a usable session_id is a contract
        // violation, not a silent empty-id "fired" success.
        let missing = Mock::new(json!({ "created": true }));
        assert!(create_session(&missing, &job(), "k").is_err());
        let blank = Mock::new(json!({ "session_id": "", "created": true }));
        assert!(create_session(&blank, &job(), "k").is_err());
    }

    #[test]
    fn create_session_includes_model_and_mode_when_set() {
        let mock = Mock::new(json!({ "session_id": "s2", "created": false }));
        let mut j = job();
        j.model_id = Some("opus".to_string());
        j.mode_id = Some("  ".to_string()); // blank -> treated as unset
        create_session(&mock, &j, "k").unwrap();

        let (_, params) = mock.last.borrow().clone().unwrap();
        assert_eq!(params["model_id"], "opus");
        assert!(params.get("mode_id").is_none());
    }
}
