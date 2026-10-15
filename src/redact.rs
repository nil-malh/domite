// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Redaction shared by logs, snapshots, diagnostic output, and secret views.
//! Extended for ns4kafka with JWT, GitLab tokens (`glpat-`, `glft-`, `gldt-`, `glrt-`),
//! Kafka reset-password fields (`newPassword`), and Connect cluster AES256 / vault keys.

use std::borrow::Cow;
use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;

use aho_corasick::{AhoCorasick, MatchKind};
use serde_json::Value;

pub const REDACTED: &str = "«redacted»";

pub const CREDENTIAL_HINTS: &[&str] = &[
    "token",
    "user-token",
    "usertoken",
    "password",
    "newpassword",
    "passwd",
    "passphrase",
    "secret",
    "apikey",
    "api-key",
    "api_key",
    "credential",
    "private-key",
    "privatekey",
    "authorization",
    "aes256key",
    "aes256salt",
    "cleartext",
];

/// Inline credential prefixes: Bearer/Basic schemes, JWT header prefix (`eyJ`),
/// URL userinfo (`://`), and GitLab token prefixes (`glpat-`, `glft-`, `gldt-`, `glrt-`, `gloas-`).
const SPECIALS: &[&str] = &[
    "bearer", "basic", "eyj", "://", "glpat-", "glft-", "gldt-", "glrt-", "gloas-",
];

const TERMINATORS: &[u8] = b" \t\r\n,;&\"'}])";
const SCHEMES: &[&str] = &["bearer", "basic", "negotiate", "digest"];

fn matcher() -> &'static AhoCorasick {
    static MATCHER: OnceLock<AhoCorasick> = OnceLock::new();
    MATCHER.get_or_init(|| {
        let patterns: Vec<&str> = CREDENTIAL_HINTS.iter().chain(SPECIALS).copied().collect();
        AhoCorasick::builder()
            .ascii_case_insensitive(true)
            .match_kind(MatchKind::LeftmostLongest)
            .build(patterns)
            .expect("static redaction patterns")
    })
}

pub fn is_credential_key(key: &str) -> bool {
    matcher()
        .find_iter(key)
        .any(|m| m.pattern().as_usize() < CREDENTIAL_HINTS.len())
}

pub fn text(input: &str) -> Cow<'_, str> {
    match credentials(input) {
        Cow::Borrowed(value) => ip_addresses(value),
        Cow::Owned(value) => match ip_addresses(&value) {
            Cow::Owned(masked) => Cow::Owned(masked),
            Cow::Borrowed(_) => Cow::Owned(value),
        },
    }
}

/// Redact credential fields inside a JSON `Value` in place (used before serializing
/// resources for snapshots or logs when needed).
pub fn redact_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if is_credential_key(k) && v.is_string() {
                    *v = Value::String(REDACTED.to_string());
                } else {
                    redact_json(v);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_json(item);
            }
        }
        _ => {}
    }
}

fn ip_addresses(input: &str) -> Cow<'_, str> {
    static CANDIDATES: OnceLock<regex::Regex> = OnceLock::new();
    let candidates = CANDIDATES
        .get_or_init(|| regex::Regex::new(r"[0-9A-Fa-f:.]+").expect("static IP address pattern"));
    let mut out: Option<String> = None;
    let mut last = 0;
    for candidate in candidates.find_iter(input) {
        let mut start = candidate.start();
        let value = candidate.as_str().trim_end_matches('.');
        let value = if value.starts_with(':') && !value.starts_with("::") {
            start += 1;
            &value[1..]
        } else {
            value
        };
        let length = if value.parse::<IpAddr>().is_ok() {
            value.len()
        } else if value.parse::<SocketAddr>().is_ok() {
            value.rfind(':').expect("socket address has a port")
        } else {
            continue;
        };
        let end = start + length;
        if input[..start].chars().next_back().is_some_and(ip_name_char)
            || input[candidate.end()..]
                .chars()
                .next()
                .is_some_and(ip_name_char)
        {
            continue;
        }
        let output = out.get_or_insert_with(|| String::with_capacity(input.len()));
        output.push_str(&input[last..start]);
        output.push_str(REDACTED);
        last = end;
    }
    match out {
        Some(mut output) => {
            output.push_str(&input[last..]);
            Cow::Owned(output)
        }
        None => Cow::Borrowed(input),
    }
}

fn ip_name_char(ch: char) -> bool {
    ch.is_alphanumeric() || matches!(ch, '_' | '-' | '.')
}

pub fn credentials(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();
    let mut out: Option<String> = None;
    let mut last = 0usize;

    for m in matcher().find_iter(input) {
        if m.start() < last {
            continue;
        }
        let span = if m.pattern().as_usize() < CREDENTIAL_HINTS.len() {
            key_value_span(bytes, m.end())
        } else {
            match SPECIALS[m.pattern().as_usize() - CREDENTIAL_HINTS.len()] {
                "://" => userinfo_span(bytes, m.end()),
                "eyj" => jwt_span(bytes, m.start()),
                s if s.starts_with("gl") => gitlab_token_span(bytes, m.start()),
                _ => scheme_span(bytes, m.end()),
            }
        };
        let Some((start, end)) = span else { continue };
        let out = out.get_or_insert_with(|| String::with_capacity(input.len()));
        out.push_str(&input[last..start]);
        out.push_str(REDACTED);
        last = end;
    }

    match out {
        Some(mut out) => {
            out.push_str(&input[last..]);
            Cow::Owned(out)
        }
        None => Cow::Borrowed(input),
    }
}

fn key_value_span(bytes: &[u8], after_key: usize) -> Option<(usize, usize)> {
    let mut i = after_key;
    while bytes
        .get(i)
        .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
    {
        i += 1;
    }
    while matches!(bytes.get(i), Some(b'"' | b'\'' | b' ' | b'\t')) {
        i += 1;
    }
    if !matches!(bytes.get(i), Some(b'=' | b':')) {
        return None;
    }
    i += 1;
    while matches!(bytes.get(i), Some(b' ' | b'\t')) {
        i += 1;
    }
    match bytes.get(i) {
        Some(&q @ (b'"' | b'\'')) => {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() {
                match bytes[end] {
                    b'\\' => end = (end + 2).min(bytes.len()),
                    c if c == q => break,
                    _ => end += 1,
                }
            }
            (end > start).then_some((start, end))
        }
        Some(_) => {
            let start = i;
            if let Some(span) = scheme_here(bytes, start) {
                return Some(span);
            }
            let end = bytes[start..]
                .iter()
                .position(|b| TERMINATORS.contains(b))
                .map_or(bytes.len(), |n| start + n);
            (end > start).then_some((start, end))
        }
        None => None,
    }
}

fn jwt_span(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    let end = bytes[at..]
        .iter()
        .position(|b| {
            !(b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'+' | b'/' | b'='))
        })
        .map_or(bytes.len(), |n| at + n);
    (end > at + 4).then_some((at, end))
}

fn gitlab_token_span(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    let end = bytes[at..]
        .iter()
        .position(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.')))
        .map_or(bytes.len(), |n| at + n);
    (end > at + 6).then_some((at, end))
}

fn scheme_span(bytes: &[u8], after: usize) -> Option<(usize, usize)> {
    let mut i = after;
    while matches!(bytes.get(i), Some(b' ' | b'\t')) {
        i += 1;
    }
    let start = i;
    let end = line_end(bytes, start);
    (end > start).then_some((start, end))
}

fn scheme_here(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    let rest = &bytes[at..];
    for scheme in SCHEMES {
        let n = scheme.len();
        if rest.len() > n
            && rest[..n].eq_ignore_ascii_case(scheme.as_bytes())
            && matches!(rest[n], b' ' | b'\t')
        {
            return scheme_span(bytes, at + n);
        }
    }
    None
}

fn userinfo_span(bytes: &[u8], after: usize) -> Option<(usize, usize)> {
    let end = bytes[after..]
        .iter()
        .position(|b| matches!(b, b'@' | b'/' | b'?' | b'#' | b' ' | b'\t' | b'\r' | b'\n'))
        .map(|n| after + n)?;
    (bytes[end] == b'@' && end > after).then_some((after, end))
}

fn line_end(bytes: &[u8], from: usize) -> usize {
    memchr::memchr2(b'\n', b'\r', &bytes[from..]).map_or(bytes.len(), |n| from + n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_jwt_and_gitlab_tokens_and_passwords() {
        assert_eq!(
            text("token=glpat-1234567890abcdef next=1").as_ref(),
            format!("token={REDACTED} next=1")
        );
        assert_eq!(
            text("failed with glpat-abcdef1234567890 in header").as_ref(),
            format!("failed with {REDACTED} in header")
        );
        assert_eq!(
            text("jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.sig here").as_ref(),
            format!("jwt {REDACTED} here")
        );
        assert_eq!(
            text(r#"{"newPassword":"s3cr3t","name":"user1"}"#).as_ref(),
            format!(r#"{{"newPassword":"{REDACTED}","name":"user1"}}"#)
        );
    }

    #[test]
    fn redacts_json_objects_recursively() {
        let mut obj = serde_json::json!({
            "kind": "ConnectCluster",
            "spec": {
                "url": "https://connect.local",
                "password": "my-secret-password",
                "aes256Key": "my-aes-key",
                "aes256Salt": "my-salt"
            }
        });
        redact_json(&mut obj);
        assert_eq!(obj["spec"]["url"], "https://connect.local");
        assert_eq!(obj["spec"]["password"], REDACTED);
        assert_eq!(obj["spec"]["aes256Key"], REDACTED);
        assert_eq!(obj["spec"]["aes256Salt"], REDACTED);
    }
}
