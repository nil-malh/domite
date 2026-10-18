// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Structured non-blocking application logging with automatic credential redaction.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::{OnceLock, mpsc};

use crate::redact;

static LOGGER: OnceLock<mpsc::SyncSender<String>> = OnceLock::new();

pub fn init(log_path: Option<PathBuf>) {
    let enabled = std::env::var("DOMITE_LOG")
        .ok()
        .is_some_and(|v| !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("off"));
    let Some(path) = log_path.or_else(|| {
        enabled.then(|| {
            dirs::state_dir()
                .or_else(dirs::cache_dir)
                .unwrap_or_else(std::env::temp_dir)
                .join("domite")
                .join("domite.log")
        })
    }) else {
        return;
    };

    let (tx, rx) = mpsc::sync_channel::<String>(256);
    if LOGGER.set(tx).is_ok() {
        std::thread::Builder::new()
            .name("domite-log".into())
            .spawn(move || {
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                else {
                    return;
                };
                while let Ok(line) = rx.recv() {
                    let _ = writeln!(file, "{line}");
                }
            })
            .ok();
    }
}

pub fn info(event: &str, fields: &[(&str, &str)]) {
    log("info", event, fields);
}

pub fn warn(event: &str, fields: &[(&str, &str)]) {
    log("warn", event, fields);
}

fn log(level: &str, event: &str, fields: &[(&str, &str)]) {
    let Some(tx) = LOGGER.get() else {
        return;
    };
    let line = format_log_line(level, event, fields);
    let _ = tx.try_send(line);
}

pub fn format_log_line(level: &str, event: &str, fields: &[(&str, &str)]) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut line = format!("ts={ts} level={level} event={event}");
    for (k, v) in fields {
        let safe = if redact::is_credential_key(k) {
            std::borrow::Cow::Borrowed(redact::REDACTED)
        } else {
            redact::text(v)
        };
        line.push(' ');
        line.push_str(k);
        line.push('=');
        if safe.contains(char::is_whitespace) || safe.contains('"') {
            line.push_str(&format!("{:?}", safe.as_ref()));
        } else {
            line.push_str(&safe);
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_logfmt_lines_and_redacts_secrets_and_ips() {
        let line = format_log_line(
            "info",
            "login",
            &[
                ("context", "prod"),
                ("user_token", "glpat-1234567890abcdef"),
                ("msg", "dial 10.20.30.40:443 with Bearer secret-jwt"),
            ],
        );
        assert!(line.contains("level=info event=login"));
        assert!(line.contains("context=prod"));
        assert!(line.contains(&format!("user_token={}", redact::REDACTED)));
        assert!(!line.contains("glpat-1234567890abcdef"));
        assert!(!line.contains("10.20.30.40"));
        assert!(!line.contains("secret-jwt"));
    }
}
