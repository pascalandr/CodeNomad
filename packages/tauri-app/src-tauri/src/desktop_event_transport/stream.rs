use super::*;

pub(super) fn open_stream(
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

pub(super) fn read_sse(
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
                    data_lines.push(data.strip_prefix(' ').unwrap_or(data).to_string());
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
