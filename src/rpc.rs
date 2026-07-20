//! Bidirectional JSON-RPC 2.0 peer over newline-delimited JSON on stdio.
//!
//! Unlike a passive command plugin, the cron worker is primarily a JSON-RPC
//! *client*: it calls the host (`config.get`, `sessions.create`) and reads the
//! matching responses. The same stdin also carries host-pushed notifications
//! (`plugin.settings.changed`), so a single reader thread demultiplexes:
//!
//! - a message with an `id` and no `method` is a response to one of our calls,
//!   routed to the blocked caller by id;
//! - a message with a `method` and no `id` is a notification, forwarded to the
//!   scheduler over a channel;
//! - a message with a `method` and an `id` is an inbound host request; we
//!   implement none, so we answer method-not-found rather than hang the host.
//!
//! stdin EOF drops the notification sender, which the scheduler observes as a
//! clean shutdown signal.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

const METHOD_NOT_FOUND: i64 = -32601;
const INTERNAL_ERROR: i64 = -32603;

/// How long a call waits for the host's response before giving up.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// A JSON-RPC error returned by the host, or a local transport failure.
#[derive(Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

/// A host-pushed notification, handed to the scheduler. Only the method is
/// used: the worker reacts to `plugin.settings.changed` by re-reading its jobs,
/// so the payload (revision, changed_keys) is not needed.
pub struct Notification {
    pub method: String,
}

/// Something the host can be asked to do. `Peer` is the real implementation;
/// tests use a fake so the typed calls in `host` can be exercised without a
/// live stdio channel.
pub trait Caller {
    fn call(&self, method: &str, params: Value) -> Result<Value, RpcError>;
}

type Pending = Arc<Mutex<HashMap<u64, Sender<Result<Value, RpcError>>>>>;
type SharedOut = Arc<Mutex<Box<dyn Write + Send>>>;

/// The live stdio peer.
pub struct Peer {
    next_id: AtomicU64,
    pending: Pending,
    out: SharedOut,
}

impl Peer {
    /// Start the reader thread over stdin/stdout and return the peer plus the
    /// notification receiver the scheduler drains.
    pub fn start() -> (Arc<Peer>, Receiver<Notification>) {
        let out: SharedOut = Arc::new(Mutex::new(Box::new(std::io::stdout())));
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (notify_tx, notify_rx) = mpsc::channel();

        let reader_pending = pending.clone();
        let reader_out = out.clone();
        thread::spawn(move || reader_loop(reader_pending, reader_out, notify_tx));

        let peer = Arc::new(Peer {
            next_id: AtomicU64::new(1),
            pending,
            out,
        });
        (peer, notify_rx)
    }

    fn write(&self, value: &Value) -> std::io::Result<()> {
        let mut out = self.out.lock().unwrap();
        writeln!(out, "{}", serde_json::to_string(value).unwrap_or_default())?;
        out.flush()
    }
}

impl Caller for Peer {
    fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id, tx);

        let request = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if let Err(e) = self.write(&request) {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError {
                code: INTERNAL_ERROR,
                message: format!("write to host failed: {e}"),
            });
        }

        match rx.recv_timeout(CALL_TIMEOUT) {
            Ok(result) => result,
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(RpcError {
                    code: INTERNAL_ERROR,
                    message: format!("timed out waiting for '{method}'"),
                })
            }
        }
    }
}

fn reader_loop(pending: Pending, out: SharedOut, notify_tx: Sender<Notification>) {
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(trimmed) else {
            continue; // ignore a malformed line rather than tear down the loop
        };

        let has_method = msg.get("method").and_then(Value::as_str).is_some();
        if !has_method {
            route_response(&pending, &msg);
            continue;
        }

        let method = msg
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match msg.get("id") {
            // A notification: no response expected.
            None | Some(Value::Null) => {
                let _ = notify_tx.send(Notification { method });
            }
            // An inbound request we do not implement.
            Some(id) => {
                let response = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": METHOD_NOT_FOUND, "message": format!("unknown method '{method}'")}
                });
                if let Ok(mut o) = out.lock() {
                    let _ = writeln!(
                        o,
                        "{}",
                        serde_json::to_string(&response).unwrap_or_default()
                    );
                    let _ = o.flush();
                }
            }
        }
    }
    // stdin closed: dropping notify_tx signals shutdown to the scheduler.
}

fn route_response(pending: &Pending, msg: &Value) {
    let Some(id) = msg.get("id").and_then(Value::as_u64) else {
        return;
    };
    let result = if let Some(err) = msg.get("error") {
        Err(RpcError {
            code: err
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or(INTERNAL_ERROR),
            message: err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        })
    } else {
        Ok(msg.get("result").cloned().unwrap_or(Value::Null))
    };
    if let Some(tx) = pending.lock().unwrap().remove(&id) {
        let _ = tx.send(result);
    }
}
