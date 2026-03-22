use parking_lot::Mutex;
use reqwest::blocking::{Client, Response};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::{AppHandle, Emitter, Manager, Url};

const EVENT_BATCH_NAME: &str = "desktop:event-batch";
const EVENT_STATUS_NAME: &str = "desktop:event-stream-status";
const FLUSH_INTERVAL_MS: u64 = 16;
const MAX_BATCH_EVENTS: usize = 256;
const DEFAULT_RECONNECT_INITIAL_DELAY_MS: u64 = 1_000;
const DEFAULT_RECONNECT_MAX_DELAY_MS: u64 = 10_000;
const DEFAULT_RECONNECT_MULTIPLIER: f64 = 2.0;
const STREAM_CONNECT_TIMEOUT_MS: u64 = 5_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesktopEventStreamConfig {
    pub base_url: String,
    pub events_url: String,
    pub cookie_name: String,
    pub session_cookie: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DesktopEventsStartRequest {
    pub reconnect: Option<DesktopEventReconnectPolicy>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DesktopEventReconnectPolicy {
    pub initial_delay_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
    pub multiplier: Option<f64>,
    pub max_attempts: Option<u32>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DesktopEventsStartResult {
    pub started: bool,
    pub generation: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct ResolvedDesktopEventReconnectPolicy {
    initial_delay_ms: u64,
    max_delay_ms: u64,
    multiplier: f64,
    max_attempts: Option<u32>,
}

impl ResolvedDesktopEventReconnectPolicy {
    fn resolve(policy: Option<&DesktopEventReconnectPolicy>) -> Self {
        let initial_delay_ms = policy
            .and_then(|value| value.initial_delay_ms)
            .unwrap_or(DEFAULT_RECONNECT_INITIAL_DELAY_MS)
            .max(1);
        let max_delay_ms = policy
            .and_then(|value| value.max_delay_ms)
            .unwrap_or(DEFAULT_RECONNECT_MAX_DELAY_MS)
            .max(initial_delay_ms);
        let multiplier = policy
            .and_then(|value| value.multiplier)
            .filter(|value| value.is_finite() && *value >= 1.0)
            .unwrap_or(DEFAULT_RECONNECT_MULTIPLIER);
        let max_attempts = policy
            .and_then(|value| value.max_attempts)
            .filter(|value| *value > 0);

        Self {
            initial_delay_ms,
            max_delay_ms,
            multiplier,
            max_attempts,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct DesktopEventTransportConfig {
    stream: DesktopEventStreamConfig,
    reconnect: ResolvedDesktopEventReconnectPolicy,
}

impl DesktopEventTransportConfig {
    fn new(stream: DesktopEventStreamConfig, request: &DesktopEventsStartRequest) -> Self {
        Self {
            stream,
            reconnect: ResolvedDesktopEventReconnectPolicy::resolve(request.reconnect.as_ref()),
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceEventBatchPayload {
    generation: u64,
    sequence: u64,
    emitted_at: u128,
    events: Vec<Value>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopEventStreamStatusPayload {
    generation: u64,
    state: &'static str,
    reconnect_attempt: u32,
    terminal: bool,
    reason: Option<String>,
    next_delay_ms: Option<u64>,
    status_code: Option<u16>,
    stats: DesktopEventTransportStats,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct DesktopEventTransportStats {
    raw_events: u64,
    emitted_events: u64,
    emitted_batches: u64,
    delta_coalesces: u64,
    snapshot_coalesces: u64,
    status_coalesces: u64,
    superseded_deltas_dropped: u64,
}

struct DesktopEventTransportState {
    generation: u64,
    stop: Option<Arc<AtomicBool>>,
    config: Option<DesktopEventTransportConfig>,
}

pub struct DesktopEventTransportManager {
    state: Arc<Mutex<DesktopEventTransportState>>,
}

enum ReaderMessage {
    Event(Value),
    End(Option<String>),
}

enum PendingEntry {
    Delta {
        key: String,
        scope: String,
        event: Value,
    },
    Status {
        key: String,
        event: Value,
    },
    Snapshot {
        key: String,
        event: Value,
    },
    Event(Value),
}

enum EventDeliveryPolicy {
    CoalesceDelta(String),
    CoalesceStatus(String),
    CoalesceSnapshot(String),
    Passthrough,
}

enum OpenStreamErrorKind {
    Unauthorized,
    Http,
    Transport,
}

struct OpenStreamError {
    kind: OpenStreamErrorKind,
    message: String,
    status_code: Option<u16>,
}

#[derive(Default)]
struct PendingBatch {
    events: Vec<PendingEntry>,
}

impl PendingBatch {
    fn push(&mut self, event: Value, stats: &mut DesktopEventTransportStats) {
        match classify_event(&event) {
            EventDeliveryPolicy::CoalesceDelta(key) => {
                let Some(scope) = delta_scope(&event) else {
                    self.events.push(PendingEntry::Event(event));
                    return;
                };

                if let Some(PendingEntry::Delta {
                    key: existing_key,
                    event: existing_event,
                    ..
                }) = self.events.last_mut()
                {
                    if existing_key == &key {
                        append_delta(existing_event, &event);
                        stats.delta_coalesces = stats.delta_coalesces.saturating_add(1);
                        return;
                    }
                }

                self.events.push(PendingEntry::Delta { key, scope, event });
            }
            EventDeliveryPolicy::CoalesceStatus(key) => {
                if let Some(PendingEntry::Status {
                    key: existing_key,
                    event: existing_event,
                }) = self.events.last_mut()
                {
                    if existing_key == &key {
                        *existing_event = event;
                        stats.status_coalesces = stats.status_coalesces.saturating_add(1);
                        return;
                    }
                }

                self.events.push(PendingEntry::Status { key, event });
            }
            EventDeliveryPolicy::CoalesceSnapshot(key) => {
                if let Some(part_scope) = snapshot_superseded_delta_scope(&event) {
                    let mut dropped = 0_u64;
                    while matches!(
                        self.events.last(),
                        Some(PendingEntry::Delta { scope, .. }) if scope == &part_scope
                    ) {
                        self.events.pop();
                        dropped = dropped.saturating_add(1);
                    }
                    if dropped > 0 {
                        stats.superseded_deltas_dropped =
                            stats.superseded_deltas_dropped.saturating_add(dropped);
                    }
                }

                if let Some(PendingEntry::Snapshot {
                    key: existing_key,
                    event: existing_event,
                }) = self.events.last_mut()
                {
                    if existing_key == &key {
                        *existing_event = event;
                        stats.snapshot_coalesces = stats.snapshot_coalesces.saturating_add(1);
                        return;
                    }
                }

                self.events.push(PendingEntry::Snapshot { key, event });
            }
            EventDeliveryPolicy::Passthrough => {
                self.events.push(PendingEntry::Event(event));
            }
        }
    }

    fn take_events(&mut self) -> Vec<Value> {
        let pending = std::mem::take(&mut self.events);
        pending
            .into_iter()
            .map(|entry| match entry {
                PendingEntry::Delta { event, .. } => event,
                PendingEntry::Status { event, .. } => event,
                PendingEntry::Snapshot { event, .. } => event,
                PendingEntry::Event(event) => event,
            })
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn pending_len(&self) -> usize {
        self.events.len()
    }
}

impl DesktopEventTransportManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(DesktopEventTransportState {
                generation: 0,
                stop: None,
                config: None,
            })),
        }
    }

    pub fn start(
        &self,
        app: AppHandle,
        stream_config: Option<DesktopEventStreamConfig>,
        request: Option<DesktopEventsStartRequest>,
    ) -> DesktopEventsStartResult {
        let Some(stream_config) = stream_config else {
            return DesktopEventsStartResult {
                started: false,
                generation: None,
                reason: Some("desktop event stream unavailable".to_string()),
            };
        };

        let request = request.unwrap_or_default();
        let transport_config = DesktopEventTransportConfig::new(stream_config, &request);

        let mut state = self.state.lock();
        if state.config.as_ref() == Some(&transport_config) {
            if let Some(stop) = &state.stop {
                if !stop.load(Ordering::SeqCst) {
                    return DesktopEventsStartResult {
                        started: true,
                        generation: Some(state.generation),
                        reason: None,
                    };
                }
            }
        }

        if let Some(stop) = state.stop.take() {
            stop.store(true, Ordering::SeqCst);
        }

        state.generation += 1;
        let generation = state.generation;
        let stop = Arc::new(AtomicBool::new(false));
        state.stop = Some(stop.clone());
        state.config = Some(transport_config.clone());
        let shared_state = self.state.clone();
        drop(state);

        thread::spawn(move || {
            run_transport_loop(app, shared_state, generation, stop, transport_config)
        });

        DesktopEventsStartResult {
            started: true,
            generation: Some(generation),
            reason: None,
        }
    }

    pub fn stop(&self) {
        let mut state = self.state.lock();
        if let Some(stop) = state.stop.take() {
            stop.store(true, Ordering::SeqCst);
        }
        state.config = None;
        state.generation += 1;
    }
}

fn run_transport_loop(
    app: AppHandle,
    state: Arc<Mutex<DesktopEventTransportState>>,
    generation: u64,
    stop: Arc<AtomicBool>,
    config: DesktopEventTransportConfig,
) {
    let mut reconnect_attempt = 0_u32;
    let mut stats = DesktopEventTransportStats::default();

    loop {
        if stop.load(Ordering::SeqCst) || !generation_matches(&state, generation) {
            break;
        }

        emit_status(
            &app,
            generation,
            "connecting",
            reconnect_attempt,
            false,
            None,
            None,
            None,
            &stats,
        );

        match open_stream(&app, &config.stream) {
            Ok(response) => {
                reconnect_attempt = 0;
                emit_status(
                    &app,
                    generation,
                    "connected",
                    reconnect_attempt,
                    false,
                    None,
                    None,
                    None,
                    &stats,
                );

                let disconnect_reason =
                    consume_stream(&app, response, &state, generation, stop.clone(), &mut stats);
                if stop.load(Ordering::SeqCst) || !generation_matches(&state, generation) {
                    break;
                }

                if !schedule_retry(
                    &app,
                    &state,
                    generation,
                    stop.clone(),
                    &config.reconnect,
                    &mut reconnect_attempt,
                    "disconnected",
                    disconnect_reason,
                    None,
                    &stats,
                ) {
                    break;
                }
            }
            Err(error) => {
                let state_name = match error.kind {
                    OpenStreamErrorKind::Unauthorized => "unauthorized",
                    OpenStreamErrorKind::Http | OpenStreamErrorKind::Transport => "error",
                };

                if !schedule_retry(
                    &app,
                    &state,
                    generation,
                    stop.clone(),
                    &config.reconnect,
                    &mut reconnect_attempt,
                    state_name,
                    Some(error.message),
                    error.status_code,
                    &stats,
                ) {
                    break;
                }
            }
        }
    }

    emit_status(
        &app,
        generation,
        "stopped",
        reconnect_attempt,
        true,
        None,
        None,
        None,
        &stats,
    );
}

fn schedule_retry(
    app: &AppHandle,
    state: &Arc<Mutex<DesktopEventTransportState>>,
    generation: u64,
    stop: Arc<AtomicBool>,
    policy: &ResolvedDesktopEventReconnectPolicy,
    reconnect_attempt: &mut u32,
    state_name: &'static str,
    reason: Option<String>,
    status_code: Option<u16>,
    stats: &DesktopEventTransportStats,
) -> bool {
    *reconnect_attempt = reconnect_attempt.saturating_add(1);
    let terminal = policy
        .max_attempts
        .map(|max_attempts| *reconnect_attempt >= max_attempts)
        .unwrap_or(false);
    let next_delay_ms = if terminal {
        None
    } else {
        Some(compute_reconnect_delay_ms(*reconnect_attempt, policy))
    };

    emit_status(
        app,
        generation,
        state_name,
        *reconnect_attempt,
        terminal,
        reason,
        next_delay_ms,
        status_code,
        stats,
    );

    if terminal {
        return false;
    }

    if let Some(delay_ms) = next_delay_ms {
        wait_with_cancellation(state, generation, stop, delay_ms);
    }

    true
}

fn wait_with_cancellation(
    state: &Arc<Mutex<DesktopEventTransportState>>,
    generation: u64,
    stop: Arc<AtomicBool>,
    delay_ms: u64,
) {
    let mut remaining_ms = delay_ms;
    while remaining_ms > 0 {
        if stop.load(Ordering::SeqCst) || !generation_matches(state, generation) {
            return;
        }

        let chunk_ms = remaining_ms.min(100);
        thread::sleep(Duration::from_millis(chunk_ms));
        remaining_ms -= chunk_ms;
    }
}

fn compute_reconnect_delay_ms(attempt: u32, policy: &ResolvedDesktopEventReconnectPolicy) -> u64 {
    let exponent = attempt.saturating_sub(1) as i32;
    let scaled = (policy.initial_delay_ms as f64) * policy.multiplier.powi(exponent);
    (scaled.round().max(policy.initial_delay_ms as f64) as u64).min(policy.max_delay_ms)
}

fn open_stream(
    app: &AppHandle,
    config: &DesktopEventStreamConfig,
) -> Result<Response, OpenStreamError> {
    let client = Client::builder()
        .connect_timeout(Duration::from_millis(STREAM_CONNECT_TIMEOUT_MS))
        .build()
        .map_err(|error| OpenStreamError {
            kind: OpenStreamErrorKind::Transport,
            message: error.to_string(),
            status_code: None,
        })?;

    let mut request = client
        .get(&config.events_url)
        .header("Accept", "text/event-stream");

    if let Some(session_cookie) = resolve_session_cookie(app, config) {
        request = request.header(
            "Cookie",
            format!("{}={}", config.cookie_name, session_cookie),
        );
    }

    let response = request.send().map_err(|error| OpenStreamError {
        kind: OpenStreamErrorKind::Transport,
        message: error.to_string(),
        status_code: None,
    })?;

    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let kind = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        OpenStreamErrorKind::Unauthorized
    } else {
        OpenStreamErrorKind::Http
    };

    Err(OpenStreamError {
        kind,
        message: format!("desktop event stream unavailable ({status})"),
        status_code: Some(status.as_u16()),
    })
}

fn resolve_session_cookie(app: &AppHandle, config: &DesktopEventStreamConfig) -> Option<String> {
    read_session_cookie_from_webview(app, &config.base_url, &config.cookie_name)
        .or_else(|| config.session_cookie.clone())
        .filter(|value| !value.is_empty())
}

fn read_session_cookie_from_webview(
    app: &AppHandle,
    base_url: &str,
    cookie_name: &str,
) -> Option<String> {
    let url = Url::parse(base_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let path = url.path();
    let windows = app.webview_windows();
    let window = windows.get("main")?;
    let cookies = window.cookies().ok()?;
    cookies
        .into_iter()
        .filter(|cookie: &tauri::webview::cookie::Cookie<'static>| cookie.name() == cookie_name)
        .filter(|cookie: &tauri::webview::cookie::Cookie<'static>| {
            let Some(domain) = cookie.domain() else {
                return true;
            };

            let normalized_domain = domain.trim_start_matches('.').to_ascii_lowercase();
            host == normalized_domain || host.ends_with(&format!(".{}", normalized_domain))
        })
        .filter(|cookie: &tauri::webview::cookie::Cookie<'static>| {
            let Some(cookie_path) = cookie.path() else {
                return true;
            };

            path.starts_with(cookie_path)
        })
        .map(|cookie: tauri::webview::cookie::Cookie<'static>| cookie.value().to_string())
        .next()
}

fn consume_stream(
    app: &AppHandle,
    response: Response,
    state: &Arc<Mutex<DesktopEventTransportState>>,
    generation: u64,
    stop: Arc<AtomicBool>,
    stats: &mut DesktopEventTransportStats,
) -> Option<String> {
    let (tx, rx) = mpsc::channel::<ReaderMessage>();
    let reader_stop = stop.clone();
    let reader_state = state.clone();
    thread::spawn(move || read_sse(response, tx, reader_stop, reader_state, generation));

    let mut pending = PendingBatch::default();
    let mut sequence = 0_u64;

    loop {
        if stop.load(Ordering::SeqCst) || !generation_matches(state, generation) {
            return Some("stopped".to_string());
        }

        match rx.recv_timeout(Duration::from_millis(FLUSH_INTERVAL_MS)) {
            Ok(ReaderMessage::Event(event)) => {
                stats.raw_events = stats.raw_events.saturating_add(1);
                pending.push(event, stats);
                if pending.pending_len() >= MAX_BATCH_EVENTS {
                    sequence += 1;
                    emit_batch(app, generation, &mut pending, sequence, state, stats);
                }
            }
            Ok(ReaderMessage::End(reason)) => {
                if !pending.is_empty() {
                    sequence += 1;
                    emit_batch(app, generation, &mut pending, sequence, state, stats);
                }
                return reason;
            }
            Err(RecvTimeoutError::Timeout) => {
                if !pending.is_empty() {
                    sequence += 1;
                    emit_batch(app, generation, &mut pending, sequence, state, stats);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                if !pending.is_empty() {
                    sequence += 1;
                    emit_batch(app, generation, &mut pending, sequence, state, stats);
                }
                return Some("reader disconnected".to_string());
            }
        }
    }
}

fn read_sse(
    response: Response,
    tx: Sender<ReaderMessage>,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<DesktopEventTransportState>>,
    generation: u64,
) {
    let mut reader = BufReader::new(response);
    let mut line = String::new();
    let mut data_lines: Vec<String> = Vec::new();

    loop {
        if stop.load(Ordering::SeqCst) || !generation_matches(&state, generation) {
            let _ = tx.send(ReaderMessage::End(Some("stopped".to_string())));
            return;
        }

        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                if let Some(event) = parse_sse_payload(&data_lines) {
                    let _ = tx.send(ReaderMessage::Event(event));
                }
                let _ = tx.send(ReaderMessage::End(Some("stream closed".to_string())));
                return;
            }
            Ok(_) => {
                let trimmed = line.trim_end_matches(['\r', '\n']);
                if trimmed.is_empty() {
                    if let Some(event) = parse_sse_payload(&data_lines) {
                        let _ = tx.send(ReaderMessage::Event(event));
                    }
                    data_lines.clear();
                    continue;
                }

                if trimmed.starts_with(':') {
                    continue;
                }

                if let Some(data) = trimmed.strip_prefix("data:") {
                    data_lines.push(data.trim_start().to_string());
                }
            }
            Err(error) => {
                if let Some(event) = parse_sse_payload(&data_lines) {
                    let _ = tx.send(ReaderMessage::Event(event));
                }
                let _ = tx.send(ReaderMessage::End(Some(error.to_string())));
                return;
            }
        }
    }
}

fn parse_sse_payload(lines: &[String]) -> Option<Value> {
    if lines.is_empty() {
        return None;
    }

    let payload = lines.join("\n").trim().to_string();
    if payload.is_empty() {
        return None;
    }

    serde_json::from_str::<Value>(&payload).ok()
}

fn emit_batch(
    app: &AppHandle,
    generation: u64,
    pending: &mut PendingBatch,
    sequence: u64,
    state: &Arc<Mutex<DesktopEventTransportState>>,
    stats: &mut DesktopEventTransportStats,
) {
    if !generation_matches(state, generation) {
        return;
    }

    let events = pending.take_events();
    if events.is_empty() {
        return;
    }

    stats.emitted_batches = stats.emitted_batches.saturating_add(1);
    stats.emitted_events = stats.emitted_events.saturating_add(events.len() as u64);

    let _ = app.emit(
        EVENT_BATCH_NAME,
        WorkspaceEventBatchPayload {
            generation,
            sequence,
            emitted_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            events,
        },
    );
}

fn emit_status(
    app: &AppHandle,
    generation: u64,
    state_name: &'static str,
    reconnect_attempt: u32,
    terminal: bool,
    reason: Option<String>,
    next_delay_ms: Option<u64>,
    status_code: Option<u16>,
    stats: &DesktopEventTransportStats,
) {
    let _ = app.emit(
        EVENT_STATUS_NAME,
        DesktopEventStreamStatusPayload {
            generation,
            state: state_name,
            reconnect_attempt,
            terminal,
            reason,
            next_delay_ms,
            status_code,
            stats: stats.clone(),
        },
    );
}

fn generation_matches(state: &Arc<Mutex<DesktopEventTransportState>>, generation: u64) -> bool {
    state.lock().generation == generation
}

fn classify_event(event: &Value) -> EventDeliveryPolicy {
    if let Some(key) = delta_key(event) {
        return EventDeliveryPolicy::CoalesceDelta(key);
    }

    if let Some(key) = status_key(event) {
        return EventDeliveryPolicy::CoalesceStatus(key);
    }

    if let Some(key) = snapshot_key(event) {
        return EventDeliveryPolicy::CoalesceSnapshot(key);
    }

    EventDeliveryPolicy::Passthrough
}

fn snapshot_key(event: &Value) -> Option<String> {
    let instance_id = event.get("instanceId")?.as_str()?;
    if event.get("type")?.as_str()? != "instance.event" {
        return None;
    }

    let inner = event.get("event")?;
    let inner_type = inner.get("type")?.as_str()?;
    let props = inner.get("properties")?;

    match inner_type {
        "message.part.updated" => {
            let session_id = props
                .get("part")
                .and_then(|part| part.get("sessionID").or_else(|| part.get("sessionId")))
                .and_then(Value::as_str)?;
            let message_id = props
                .get("part")
                .and_then(|part| part.get("messageID").or_else(|| part.get("messageId")))
                .and_then(Value::as_str)?;
            let part_id = props
                .get("part")
                .and_then(|part| part.get("id"))
                .and_then(Value::as_str)?;

            Some(format!(
                "message.part.updated:{}:{}:{}:{}",
                instance_id, session_id, message_id, part_id
            ))
        }
        "message.updated" => {
            let info = props.get("info")?;
            let session_id = info
                .get("sessionID")
                .or_else(|| info.get("sessionId"))
                .and_then(Value::as_str)?;
            let message_id = info.get("id").and_then(Value::as_str)?;

            Some(format!(
                "message.updated:{}:{}:{}",
                instance_id, session_id, message_id
            ))
        }
        "session.updated" | "session.status" => {
            let session_id = props
                .get("info")
                .and_then(|info| info.get("id"))
                .and_then(Value::as_str)
                .or_else(|| {
                    props
                        .get("sessionID")
                        .or_else(|| props.get("sessionId"))
                        .and_then(Value::as_str)
                })?;

            Some(format!("{}:{}:{}", inner_type, instance_id, session_id))
        }
        _ => None,
    }
}

fn delta_scope(event: &Value) -> Option<String> {
    let instance_id = event.get("instanceId")?.as_str()?;
    if event.get("type")?.as_str()? != "instance.event" {
        return None;
    }

    let inner = event.get("event")?;
    if inner.get("type")?.as_str()? != "message.part.delta" {
        return None;
    }

    let props = inner.get("properties")?;
    let session_id = props
        .get("sessionID")
        .or_else(|| props.get("sessionId"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message_id = props
        .get("messageID")
        .or_else(|| props.get("messageId"))
        .and_then(Value::as_str)?;
    let part_id = props
        .get("partID")
        .or_else(|| props.get("partId"))
        .and_then(Value::as_str)?;

    Some(format!(
        "message.part:{}:{}:{}:{}",
        instance_id, session_id, message_id, part_id
    ))
}

fn delta_key(event: &Value) -> Option<String> {
    let scope = delta_scope(event)?;
    let props = event.get("event")?.get("properties")?;
    let field = props.get("field")?.as_str()?;

    Some(format!("{}:{}", scope, field))
}

fn snapshot_superseded_delta_scope(event: &Value) -> Option<String> {
    let instance_id = event.get("instanceId")?.as_str()?;
    if event.get("type")?.as_str()? != "instance.event" {
        return None;
    }

    let inner = event.get("event")?;
    if inner.get("type")?.as_str()? != "message.part.updated" {
        return None;
    }

    let part = inner.get("properties")?.get("part")?;
    let session_id = part
        .get("sessionID")
        .or_else(|| part.get("sessionId"))
        .and_then(Value::as_str)?;
    let message_id = part
        .get("messageID")
        .or_else(|| part.get("messageId"))
        .and_then(Value::as_str)?;
    let part_id = part.get("id")?.as_str()?;

    Some(format!(
        "message.part:{}:{}:{}:{}",
        instance_id, session_id, message_id, part_id
    ))
}

fn append_delta(target: &mut Value, event: &Value) {
    let next_delta = event
        .get("event")
        .and_then(|value| value.get("properties"))
        .and_then(|value| value.get("delta"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    if let Some(existing_delta) = target
        .get_mut("event")
        .and_then(Value::as_object_mut)
        .and_then(|event| event.get_mut("properties"))
        .and_then(Value::as_object_mut)
        .and_then(|props| props.get_mut("delta"))
    {
        let combined = existing_delta.as_str().unwrap_or_default().to_string() + next_delta;
        *existing_delta = Value::String(combined);
    }
}

fn status_key(event: &Value) -> Option<String> {
    if event.get("type")?.as_str()? != "instance.eventStatus" {
        return None;
    }

    Some(event.get("instanceId")?.as_str()?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fresh_stats() -> DesktopEventTransportStats {
        DesktopEventTransportStats::default()
    }

    fn delta_event(delta: &str) -> Value {
        json!({
            "type": "instance.event",
            "instanceId": "inst-1",
            "event": {
                "type": "message.part.delta",
                "properties": {
                    "sessionID": "sess-1",
                    "messageID": "msg-1",
                    "partID": "part-1",
                    "field": "text",
                    "delta": delta,
                }
            }
        })
    }

    fn delta_event_for(part_id: &str, delta: &str) -> Value {
        json!({
            "type": "instance.event",
            "instanceId": "inst-1",
            "event": {
                "type": "message.part.delta",
                "properties": {
                    "sessionID": "sess-1",
                    "messageID": "msg-1",
                    "partID": part_id,
                    "field": "text",
                    "delta": delta,
                }
            }
        })
    }

    fn message_part_updated_event(text: &str) -> Value {
        json!({
            "type": "instance.event",
            "instanceId": "inst-1",
            "event": {
                "type": "message.part.updated",
                "properties": {
                    "part": {
                        "id": "part-1",
                        "type": "text",
                        "text": text,
                        "sessionID": "sess-1",
                        "messageID": "msg-1"
                    }
                }
            }
        })
    }

    #[test]
    fn coalesces_message_part_delta_events() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(delta_event("Hello"), &mut stats);
        pending.push(delta_event(" world"), &mut stats);

        let events = pending.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["event"]["properties"]["delta"].as_str(),
            Some("Hello world")
        );
    }

    #[test]
    fn last_write_wins_for_status_events() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(
            json!({
                "type": "instance.eventStatus",
                "instanceId": "inst-1",
                "status": "connecting"
            }),
            &mut stats,
        );
        pending.push(
            json!({
                "type": "instance.eventStatus",
                "instanceId": "inst-1",
                "status": "connected"
            }),
            &mut stats,
        );

        let events = pending.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["status"].as_str(), Some("connected"));
    }

    #[test]
    fn last_write_wins_for_consecutive_snapshot_events() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(message_part_updated_event("Hello"), &mut stats);
        pending.push(message_part_updated_event("Hello world"), &mut stats);

        let events = pending.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["event"]["properties"]["part"]["text"].as_str(),
            Some("Hello world")
        );
    }

    #[test]
    fn interleaved_snapshot_keys_keep_order() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(message_part_updated_event("A1"), &mut stats);
        pending.push(
            json!({
                "type": "instance.event",
                "instanceId": "inst-1",
                "event": {
                    "type": "message.part.updated",
                    "properties": {
                        "part": {
                            "id": "part-2",
                            "type": "text",
                            "text": "B1",
                            "sessionID": "sess-1",
                            "messageID": "msg-1"
                        }
                    }
                }
            }),
            &mut stats,
        );
        pending.push(message_part_updated_event("A2"), &mut stats);

        let events = pending.take_events();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0]["event"]["properties"]["part"]["id"].as_str(),
            Some("part-1")
        );
        assert_eq!(
            events[1]["event"]["properties"]["part"]["id"].as_str(),
            Some("part-2")
        );
        assert_eq!(
            events[2]["event"]["properties"]["part"]["text"].as_str(),
            Some("A2")
        );
    }

    #[test]
    fn snapshot_replaces_trailing_deltas_for_same_part() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(delta_event("Hello"), &mut stats);
        pending.push(message_part_updated_event("Hello world"), &mut stats);

        let events = pending.take_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["event"]["type"].as_str(),
            Some("message.part.updated")
        );
        assert_eq!(
            events[0]["event"]["properties"]["part"]["text"].as_str(),
            Some("Hello world")
        );
    }

    #[test]
    fn structural_events_force_coalesced_flush_before_append() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(delta_event("Hello"), &mut stats);
        pending.push(
            json!({
                "type": "instance.event",
                "instanceId": "inst-1",
                "event": {
                    "type": "message.updated",
                    "properties": {
                        "id": "msg-1"
                    }
                }
            }),
            &mut stats,
        );

        let events = pending.take_events();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0]["event"]["type"].as_str(),
            Some("message.part.delta")
        );
        assert_eq!(events[1]["event"]["type"].as_str(), Some("message.updated"));
    }

    #[test]
    fn interleaved_delta_keys_keep_order() {
        let mut pending = PendingBatch::default();
        let mut stats = fresh_stats();
        pending.push(delta_event_for("part-1", "A1"), &mut stats);
        pending.push(delta_event_for("part-2", "B1"), &mut stats);
        pending.push(delta_event_for("part-1", "A2"), &mut stats);

        let events = pending.take_events();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0]["event"]["properties"]["partID"].as_str(),
            Some("part-1")
        );
        assert_eq!(
            events[0]["event"]["properties"]["delta"].as_str(),
            Some("A1")
        );
        assert_eq!(
            events[1]["event"]["properties"]["partID"].as_str(),
            Some("part-2")
        );
        assert_eq!(
            events[1]["event"]["properties"]["delta"].as_str(),
            Some("B1")
        );
        assert_eq!(
            events[2]["event"]["properties"]["partID"].as_str(),
            Some("part-1")
        );
        assert_eq!(
            events[2]["event"]["properties"]["delta"].as_str(),
            Some("A2")
        );
    }

    #[test]
    fn reconnect_delay_grows_and_caps() {
        let policy = ResolvedDesktopEventReconnectPolicy {
            initial_delay_ms: 100,
            max_delay_ms: 500,
            multiplier: 2.0,
            max_attempts: None,
        };

        assert_eq!(compute_reconnect_delay_ms(1, &policy), 100);
        assert_eq!(compute_reconnect_delay_ms(2, &policy), 200);
        assert_eq!(compute_reconnect_delay_ms(3, &policy), 400);
        assert_eq!(compute_reconnect_delay_ms(4, &policy), 500);
    }
}
