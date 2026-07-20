//! Contract test: drive the built worker as the host would over stdio, and
//! assert the bidirectional JSON-RPC handshake without waiting for a cron fire
//! (the due-logic is unit-tested in `schedule.rs`). We act as the JSON-RPC
//! server: read the worker's outbound requests, answer them, push a
//! notification, and check an inbound request is answered rather than hung.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

const BIN: &str = env!("CARGO_BIN_EXE_plugin-cron-worker");
const TIMEOUT: Duration = Duration::from_secs(5);

/// A minimal stand-in for the host: reads the worker's stdout line by line and
/// writes responses to its stdin.
struct Harness {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
}

impl Harness {
    fn spawn() -> Harness {
        let mut child = Command::new(BIN)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn worker");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(l) = line else { break };
                if tx.send(l).is_err() {
                    break;
                }
            }
        });
        Harness {
            child,
            stdin,
            lines: rx,
        }
    }

    fn next_message(&self) -> Value {
        loop {
            let line = self
                .lines
                .recv_timeout(TIMEOUT)
                .expect("worker produced a message in time");
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_str(trimmed).expect("worker line is valid json");
        }
    }

    fn send(&mut self, value: Value) {
        writeln!(self.stdin, "{}", serde_json::to_string(&value).unwrap()).expect("write stdin");
        self.stdin.flush().expect("flush stdin");
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn worker_reads_jobs_and_reacts_to_settings_change() {
    let mut host = Harness::spawn();

    // 1. On startup the worker reads its jobs setting.
    let req = host.next_message();
    assert_eq!(req["method"], "config.get", "got: {req}");
    assert_eq!(req["params"]["key"], "jobs");
    let id = req["id"].clone();
    host.send(json!({"jsonrpc": "2.0", "id": id, "result": {"value": null, "revision": 0}}));

    // 2. A settings-change push wakes the worker to re-read the jobs.
    host.send(json!({
        "jsonrpc": "2.0",
        "method": "plugin.settings.changed",
        "params": {"revision": 1, "changed_keys": ["jobs"]}
    }));
    let req2 = host.next_message();
    assert_eq!(req2["method"], "config.get", "got: {req2}");
    let id2 = req2["id"].clone();
    host.send(json!({"jsonrpc": "2.0", "id": id2, "result": {"value": null, "revision": 1}}));

    // 3. An inbound request the worker does not implement is answered, not hung.
    host.send(json!({"jsonrpc": "2.0", "id": 42, "method": "cron.unknown"}));
    let resp = host.next_message();
    assert_eq!(resp["id"], 42, "got: {resp}");
    assert_eq!(resp["error"]["code"], -32601, "got: {resp}");
}
