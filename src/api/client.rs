//! ns4kafka REST API client (`HttpClient`) with JWT caching and automatic re-login,
//! plus an in-memory `FakeClient` for `--demo`, `--snapshot`, and unit/integration tests.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::api::config::{KafkaContext, jwt_cache_path};
use crate::api::discovery::{DiscoveryRegistry, ResourceDefinition};
use crate::atomicfile;
use crate::redact;
use crate::store::PollFailure;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub message: String,
    pub failure: PollFailure,
    pub status: Option<u16>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", redact::credentials(&self.message))
    }
}

impl ApiError {
    pub fn from_status(status: StatusCode, body: &str) -> Self {
        let failure = match status {
            StatusCode::UNAUTHORIZED => PollFailure::Unauthorized,
            StatusCode::FORBIDDEN => PollFailure::Forbidden,
            _ => PollFailure::Server,
        };
        let detail = extract_status_message(body).unwrap_or_else(|| {
            if body.trim().is_empty() {
                status.to_string()
            } else {
                body.trim().to_string()
            }
        });
        Self {
            message: format!("HTTP {}: {}", status.as_u16(), detail),
            failure,
            status: Some(status.as_u16()),
        }
    }

    pub fn network(err: impl std::error::Error) -> Self {
        let mut msg = err.to_string();
        let mut source = err.source();
        while let Some(s) = source {
            let s_str = s.to_string();
            if !msg.contains(&s_str) {
                msg.push_str(": ");
                msg.push_str(&s_str);
            }
            source = s.source();
        }
        Self {
            message: msg,
            failure: PollFailure::Network,
            status: None,
        }
    }

    pub fn parse_err(err: impl std::fmt::Display) -> Self {
        Self {
            message: err.to_string(),
            failure: PollFailure::Network,
            status: None,
        }
    }
}

fn extract_status_message(body: &str) -> Option<String> {
    let val: Value = serde_json::from_str(body).ok()?;
    let msg = val
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let causes = val
        .pointer("/details/causes")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();
    if !causes.is_empty() && !msg.is_empty() {
        Some(format!("{msg} ({causes})"))
    } else if !causes.is_empty() {
        Some(causes)
    } else if !msg.is_empty() {
        Some(msg.to_string())
    } else {
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub username: String,
    pub active: bool,
    pub exp: Option<i64>,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub status: String,
    pub resource: Value,
}

#[async_trait]
pub trait Ns4KafkaClient: Send + Sync {
    fn context(&self) -> &KafkaContext;
    async fn login(&self) -> Result<TokenInfo, ApiError>;
    async fn token_info(&self) -> Result<TokenInfo, ApiError>;
    async fn server_version(&self) -> Result<String, ApiError>;
    async fn api_resources(&self) -> Result<Vec<ResourceDefinition>, ApiError>;
    async fn list_namespaces(&self) -> Result<Vec<(String, bool)>, ApiError>;
    async fn list_resources(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
    ) -> Result<Vec<Value>, ApiError>;
    async fn apply_resource(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
        body: &Value,
        dryrun: bool,
    ) -> Result<ApplyResult, ApiError>;
    async fn delete_resource(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
        name: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError>;
    async fn import_resources(
        &self,
        namespace: &str,
        path: &str,
        name_glob: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError>;
    async fn delete_records(
        &self,
        namespace: &str,
        topic: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError>;
    async fn change_connector_state(
        &self,
        namespace: &str,
        connector: &str,
        action: &str,
    ) -> Result<Value, ApiError>;
    async fn list_connector_offsets(
        &self,
        namespace: &str,
        connector: &str,
    ) -> Result<Vec<Value>, ApiError>;
    async fn reset_connector_offsets(
        &self,
        namespace: &str,
        connector: &str,
    ) -> Result<Value, ApiError>;
    async fn reset_consumer_group_offsets(
        &self,
        namespace: &str,
        group: &str,
        topic: &str,
        method: &str,
        options: Option<&str>,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError>;
    async fn delete_consumer_group(
        &self,
        namespace: &str,
        group: &str,
        dryrun: bool,
    ) -> Result<(), ApiError>;
    async fn update_schema_compatibility(
        &self,
        namespace: &str,
        subject: &str,
        compatibility: Option<&str>,
    ) -> Result<Value, ApiError>;
    async fn get_schema(&self, namespace: &str, subject: &str) -> Result<Value, ApiError>;
    async fn get_consumer_group(&self, namespace: &str, group: &str) -> Result<Value, ApiError>;
    async fn reset_user_password(&self, namespace: &str, user: &str) -> Result<Value, ApiError>;
    async fn vault_passwords(
        &self,
        namespace: &str,
        connect_cluster: &str,
        passwords: &[String],
    ) -> Result<Vec<Value>, ApiError>;
}

// ---------------------------------------------------------------------------
// Real HTTP client (reqwest + rustls + JWT disk cache)
// ---------------------------------------------------------------------------

pub struct HttpClient {
    ctx: KafkaContext,
    http: reqwest::Client,
    jwt: RwLock<Option<String>>,
    login_info: RwLock<Option<TokenInfo>>,
}

#[derive(Deserialize)]
struct LoginResponse {
    access_token: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

fn parse_jwt_claims(token: &str) -> Option<TokenInfo> {
    use base64::Engine as _;
    let payload_b64 = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload_b64))
        .ok()?;
    let val: Value = serde_json::from_slice(&bytes).ok()?;
    let exp = val.get("exp").and_then(Value::as_i64);
    if let Some(exp_ts) = exp
        && exp_ts <= chrono::Utc::now().timestamp() + 10
    {
        // Token is already expired
        return None;
    }
    let username = val
        .get("sub")
        .or_else(|| val.get("username"))
        .and_then(Value::as_str)
        .unwrap_or("user")
        .to_string();
    let roles = val
        .get("roles")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Some(TokenInfo {
        username,
        active: true,
        exp,
        roles,
    })
}

fn load_cached_jwt(context_name: &str) -> (Option<String>, Option<TokenInfo>) {
    // 1. Check domite's own per-context cache (`~/.cache/domite/jwt/<ctx>.jwt`)
    if let Ok(raw) = std::fs::read_to_string(jwt_cache_path(context_name)) {
        let token = raw.trim().to_string();
        if let Some(info) = parse_jwt_claims(&token) {
            return (Some(token), Some(info));
        }
    }
    // 2. Fallback to kafkactl's `~/.kafkactl/jwt` if present and unexpired
    if let Some(home) = dirs::home_dir() {
        let kc_jwt_path = home.join(".kafkactl").join("jwt");
        if let Ok(raw) = std::fs::read_to_string(kc_jwt_path) {
            if let Ok(parsed) = serde_json::from_str::<LoginResponse>(&raw)
                && let Some(mut info) = parse_jwt_claims(&parsed.access_token)
            {
                if let Some(u) = parsed.username {
                    info.username = u;
                }
                if !parsed.roles.is_empty() {
                    info.roles = parsed.roles;
                }
                return (Some(parsed.access_token), Some(info));
            } else if let Some(info) = parse_jwt_claims(raw.trim()) {
                return (Some(raw.trim().to_string()), Some(info));
            }
        }
    }
    (None, None)
}

impl HttpClient {
    pub fn new(ctx: KafkaContext) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| e.to_string())?;

        let (cached_jwt, cached_info) = load_cached_jwt(&ctx.name);

        Ok(Self {
            ctx,
            http,
            jwt: RwLock::new(cached_jwt),
            login_info: RwLock::new(cached_info),
        })
    }

    fn base_url(&self) -> &str {
        self.ctx.api.trim_end_matches('/')
    }

    async fn ensure_jwt(&self) -> Result<String, ApiError> {
        if let Some(tok) = self.jwt.read().ok().and_then(|g| g.clone()) {
            // Verify it hasn't expired since we cached it
            if parse_jwt_claims(&tok).is_some() {
                return Ok(tok);
            }
        }
        self.perform_login().await
    }

    async fn perform_login(&self) -> Result<String, ApiError> {
        let url = format!("{}/login", self.base_url());
        let payload = json!({
            "username": self.ctx.username,
            "password": self.ctx.user_token,
        });
        let resp = self
            .http
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(ApiError::network)?;
        let status = resp.status();
        let body = resp.text().await.map_err(ApiError::network)?;
        if !status.is_success() {
            return Err(ApiError::from_status(status, &body));
        }
        let parsed: LoginResponse = serde_json::from_str(&body)
            .map_err(|e| ApiError::from_status(status, &e.to_string()))?;
        let token = parsed.access_token;
        let mut info = parse_jwt_claims(&token).unwrap_or(TokenInfo {
            username: self.ctx.username.clone(),
            active: true,
            exp: parsed
                .expires_in
                .map(|d| chrono::Utc::now().timestamp() + d),
            roles: Vec::new(),
        });
        if let Some(u) = parsed.username {
            info.username = u;
        }
        if !parsed.roles.is_empty() {
            info.roles = parsed.roles;
        }
        if let Ok(mut guard) = self.jwt.write() {
            *guard = Some(token.clone());
        }
        if let Ok(mut info_guard) = self.login_info.write() {
            *info_guard = Some(info);
        }
        let _ = atomicfile::write_secret(&jwt_cache_path(&self.ctx.name), &token);
        Ok(token)
    }

    async fn send_authed(
        &self,
        method: reqwest::Method,
        path_and_query: &str,
        body: Option<&Value>,
    ) -> Result<(StatusCode, reqwest::header::HeaderMap, String), ApiError> {
        let mut token = self.ensure_jwt().await?;
        let url = format!("{}{}", self.base_url(), path_and_query);

        for attempt in 0..2 {
            let mut req = self.http.request(method.clone(), &url).bearer_auth(&token);
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = req.send().await.map_err(ApiError::network)?;
            let status = resp.status();
            let headers = resp.headers().clone();
            let text = resp.text().await.map_err(ApiError::network)?;
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                token = self.perform_login().await?;
                continue;
            }
            if !status.is_success() {
                return Err(ApiError::from_status(status, &text));
            }
            return Ok((status, headers, text));
        }
        Err(ApiError {
            message: "Authentication failed after retry".into(),
            failure: PollFailure::Unauthorized,
            status: Some(401),
        })
    }
}

#[async_trait]
impl Ns4KafkaClient for HttpClient {
    fn context(&self) -> &KafkaContext {
        &self.ctx
    }

    async fn login(&self) -> Result<TokenInfo, ApiError> {
        let _ = self.perform_login().await?;
        self.token_info().await
    }

    async fn token_info(&self) -> Result<TokenInfo, ApiError> {
        if let Some(info) = self.login_info.read().ok().and_then(|g| g.clone()) {
            return Ok(info);
        }
        match self
            .send_authed(reqwest::Method::GET, "/token_info", None)
            .await
        {
            Ok((_, _, text)) => {
                let val: Value = serde_json::from_str(&text).unwrap_or_default();
                let username = val
                    .get("username")
                    .or_else(|| val.get("sub"))
                    .and_then(Value::as_str)
                    .unwrap_or(&self.ctx.username)
                    .to_string();
                let active = val.get("active").and_then(Value::as_bool).unwrap_or(true);
                let exp = val.get("exp").and_then(Value::as_i64);
                let roles = val
                    .get("roles")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(TokenInfo {
                    username,
                    active,
                    exp,
                    roles,
                })
            }
            Err(_) => Ok(TokenInfo {
                username: self.ctx.username.clone(),
                active: true,
                exp: None,
                roles: Vec::new(),
            }),
        }
    }

    async fn server_version(&self) -> Result<String, ApiError> {
        let (_, _, text) = self
            .send_authed(reqwest::Method::GET, "/api/app-info/version", None)
            .await?;
        let val: Value = serde_json::from_str(&text).unwrap_or_default();
        Ok(val
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or(text.trim())
            .to_string())
    }

    async fn api_resources(&self) -> Result<Vec<ResourceDefinition>, ApiError> {
        let (_, _, text) = self
            .send_authed(reqwest::Method::GET, "/api-resources", None)
            .await?;
        let defs: Vec<ResourceDefinition> = serde_json::from_str(&text)
            .map_err(|e| ApiError::parse_err(format!("invalid /api-resources JSON: {e}")))?;
        Ok(defs)
    }

    async fn list_namespaces(&self) -> Result<Vec<(String, bool)>, ApiError> {
        let (_, _, text) = self
            .send_authed(reqwest::Method::GET, "/api/namespaces", None)
            .await?;
        let items: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
        let mut names: Vec<(String, bool)> = items
            .iter()
            .filter_map(|v| {
                v.pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .map(|s| (s.to_string(), false))
            })
            .collect();
        if !self.ctx.namespace.is_empty() && !names.iter().any(|(n, _)| n == &self.ctx.namespace) {
            names.push((self.ctx.namespace.clone(), false));
        }
        names.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(names)
    }

    async fn list_resources(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
    ) -> Result<Vec<Value>, ApiError> {
        if def.kind == "ConsumerGroup" {
            let ns = namespace
                .filter(|n| !n.is_empty() && *n != "*")
                .unwrap_or(&self.ctx.namespace);
            let owned_path = format!(
                "/api/namespaces/{}/consumer-groups",
                urlencoding::encode(ns)
            );
            let (_, _, owned_text) = self
                .send_authed(reqwest::Method::GET, &owned_path, None)
                .await?;
            let owned_raw: Vec<Value> = serde_json::from_str(&owned_text)
                .map_err(|e| ApiError::parse_err(format!("invalid consumer-groups JSON: {e}")))?;
            let mut groups = aggregate_consumer_groups(owned_raw, "Owned");

            let ext_path = format!(
                "/api/namespaces/{}/consumer-groups/_/external",
                urlencoding::encode(ns)
            );
            if let Ok((_, _, ext_text)) = self
                .send_authed(reqwest::Method::GET, &ext_path, None)
                .await
                && let Ok(ext_raw) = serde_json::from_str::<Vec<Value>>(&ext_text)
            {
                groups.extend(aggregate_consumer_groups(ext_raw, "External"));
            }
            return Ok(groups);
        }

        if def.kind == "ExternalConsumerGroup" {
            let ns = namespace
                .filter(|n| !n.is_empty() && *n != "*")
                .unwrap_or(&self.ctx.namespace);
            let ext_path = format!(
                "/api/namespaces/{}/consumer-groups/_/external",
                urlencoding::encode(ns)
            );
            let (_, _, ext_text) = self
                .send_authed(reqwest::Method::GET, &ext_path, None)
                .await?;
            let ext_raw: Vec<Value> = serde_json::from_str(&ext_text).map_err(|e| {
                ApiError::parse_err(format!("invalid external consumer-groups JSON: {e}"))
            })?;
            return Ok(aggregate_consumer_groups(ext_raw, "External"));
        }

        let path = if !def.namespaced || def.path == "namespaces" {
            "/api/namespaces".to_string()
        } else {
            match namespace {
                Some(ns) if !ns.is_empty() && ns != "*" => {
                    format!("/api/namespaces/{}/{}", urlencoding::encode(ns), def.path)
                }
                _ => format!("/api/{}", def.path),
            }
        };
        let (_, _, text) = self.send_authed(reqwest::Method::GET, &path, None).await?;
        let raw_list: Vec<Value> = serde_json::from_str(&text)
            .map_err(|e| ApiError::parse_err(format!("invalid list JSON: {e}")))?;
        Ok(expand_quota_rows(&def.kind, raw_list))
    }

    async fn apply_resource(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
        body: &Value,
        dryrun: bool,
    ) -> Result<ApplyResult, ApiError> {
        let mut clean = body.clone();
        if let Some(obj) = clean.as_object_mut() {
            obj.remove("_quotaKey");
            obj.remove("_quotaValue");
            obj.remove("_quotaUsed");
        }
        let ns = body
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .or(namespace)
            .unwrap_or(&self.ctx.namespace);
        let path = if !def.namespaced || def.path == "namespaces" {
            format!("/api/namespaces?dryrun={dryrun}")
        } else {
            format!(
                "/api/namespaces/{}/{}?dryrun={dryrun}",
                urlencoding::encode(ns),
                def.path
            )
        };
        let (_, headers, text) = self
            .send_authed(reqwest::Method::POST, &path, Some(&clean))
            .await?;
        let status = headers
            .get("X-Ns4kafka-Result")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(if dryrun { "dry-run" } else { "changed" })
            .to_string();
        let resource: Value = serde_json::from_str(&text).unwrap_or(clean);
        Ok(ApplyResult { status, resource })
    }

    async fn delete_resource(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
        name: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let ns = namespace.unwrap_or(&self.ctx.namespace);
        let path = if def.kind == "ConsumerGroup" {
            format!(
                "/api/namespaces/{}/consumer-groups/{}?dryrun={dryrun}",
                urlencoding::encode(ns),
                urlencoding::encode(name)
            )
        } else if !def.namespaced || def.path == "namespaces" {
            format!(
                "/api/namespaces?name={}&dryrun={dryrun}",
                urlencoding::encode(name)
            )
        } else {
            format!(
                "/api/namespaces/{}/{}?name={}&dryrun={dryrun}",
                urlencoding::encode(ns),
                def.path,
                urlencoding::encode(name)
            )
        };
        let (_, _, text) = self
            .send_authed(reqwest::Method::DELETE, &path, None)
            .await?;
        if text.trim().is_empty() {
            return Ok(vec![
                json!({ "metadata": { "name": name, "namespace": ns } }),
            ]);
        }
        let parsed: Value = serde_json::from_str(&text).unwrap_or_default();
        match parsed {
            Value::Array(arr) => Ok(arr),
            other => Ok(vec![other]),
        }
    }

    async fn import_resources(
        &self,
        namespace: &str,
        path: &str,
        name_glob: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let url = format!(
            "/api/namespaces/{}/{}/_/import?name={}&dryrun={dryrun}",
            urlencoding::encode(namespace),
            path,
            urlencoding::encode(name_glob)
        );
        let (_, _, text) = self.send_authed(reqwest::Method::POST, &url, None).await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn delete_records(
        &self,
        namespace: &str,
        topic: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let url = format!(
            "/api/namespaces/{}/topics/{}/delete-records?dryrun={dryrun}",
            urlencoding::encode(namespace),
            urlencoding::encode(topic)
        );
        let (_, _, text) = self.send_authed(reqwest::Method::POST, &url, None).await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn change_connector_state(
        &self,
        namespace: &str,
        connector: &str,
        action: &str,
    ) -> Result<Value, ApiError> {
        let url = format!(
            "/api/namespaces/{}/connectors/{}/change-state",
            urlencoding::encode(namespace),
            urlencoding::encode(connector)
        );
        let body = json!({
            "apiVersion": "v1",
            "kind": "ChangeConnectorState",
            "metadata": {
                "name": connector,
                "namespace": namespace
            },
            "spec": {
                "action": action
            }
        });
        let (_, _, text) = self
            .send_authed(reqwest::Method::POST, &url, Some(&body))
            .await?;
        Ok(serde_json::from_str(&text).unwrap_or(body))
    }

    async fn list_connector_offsets(
        &self,
        namespace: &str,
        connector: &str,
    ) -> Result<Vec<Value>, ApiError> {
        let url = format!(
            "/api/namespaces/{}/connectors/{}/offsets",
            urlencoding::encode(namespace),
            urlencoding::encode(connector)
        );
        let (_, _, text) = self.send_authed(reqwest::Method::GET, &url, None).await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn reset_connector_offsets(
        &self,
        namespace: &str,
        connector: &str,
    ) -> Result<Value, ApiError> {
        let url = format!(
            "/api/namespaces/{}/connectors/{}/offsets",
            urlencoding::encode(namespace),
            urlencoding::encode(connector)
        );
        let (_, _, text) = self
            .send_authed(reqwest::Method::DELETE, &url, None)
            .await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn reset_consumer_group_offsets(
        &self,
        namespace: &str,
        group: &str,
        topic: &str,
        method: &str,
        options: Option<&str>,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let url = format!(
            "/api/namespaces/{}/consumer-groups/{}/reset?dryrun={dryrun}",
            urlencoding::encode(namespace),
            urlencoding::encode(group)
        );
        let body = json!({
            "apiVersion": "v1",
            "kind": "ConsumerGroupResetOffsets",
            "metadata": {
                "name": group,
                "namespace": namespace
            },
            "spec": {
                "topic": topic,
                "method": method,
                "options": options
            }
        });
        let (_, _, text) = self
            .send_authed(reqwest::Method::POST, &url, Some(&body))
            .await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn delete_consumer_group(
        &self,
        namespace: &str,
        group: &str,
        dryrun: bool,
    ) -> Result<(), ApiError> {
        let url = format!(
            "/api/namespaces/{}/consumer-groups/{}?dryrun={dryrun}",
            urlencoding::encode(namespace),
            urlencoding::encode(group)
        );
        let _ = self
            .send_authed(reqwest::Method::DELETE, &url, None)
            .await?;
        Ok(())
    }

    async fn update_schema_compatibility(
        &self,
        namespace: &str,
        subject: &str,
        compatibility: Option<&str>,
    ) -> Result<Value, ApiError> {
        let url = format!(
            "/api/namespaces/{}/schemas/{}/config",
            urlencoding::encode(namespace),
            urlencoding::encode(subject)
        );
        let (_, _, text) = match compatibility {
            Some(compat) if !compat.eq_ignore_ascii_case("GLOBAL") => {
                let body = json!({ "compatibility": compat });
                self.send_authed(reqwest::Method::POST, &url, Some(&body))
                    .await?
            }
            _ => {
                self.send_authed(reqwest::Method::DELETE, &url, None)
                    .await?
            }
        };
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn get_schema(&self, namespace: &str, subject: &str) -> Result<Value, ApiError> {
        let url = format!(
            "/api/namespaces/{}/schemas/{}",
            urlencoding::encode(namespace),
            urlencoding::encode(subject)
        );
        let (_, _, text) = self.send_authed(reqwest::Method::GET, &url, None).await?;
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|e| ApiError::parse_err(format!("invalid schema JSON: {e}")))?;
        match parsed {
            Value::Array(mut arr) => arr
                .drain(..)
                .next()
                .ok_or_else(|| ApiError::parse_err("empty schema array")),
            other => Ok(other),
        }
    }

    async fn get_consumer_group(&self, namespace: &str, group: &str) -> Result<Value, ApiError> {
        let url = format!(
            "/api/namespaces/{}/consumer-groups?name={}",
            urlencoding::encode(namespace),
            urlencoding::encode(group)
        );
        let (_, _, text) = self.send_authed(reqwest::Method::GET, &url, None).await?;
        let raw_items: Vec<Value> = serde_json::from_str(&text)
            .map_err(|e| ApiError::parse_err(format!("invalid consumer-group detail JSON: {e}")))?;
        let mut aggregated = aggregate_consumer_groups(raw_items, "Owned");
        if let Some(mut first) = aggregated.drain(..).next() {
            // Mark as hydrated even if the group has 0 committed offsets (e.g. brand-new EMPTY group)
            if first.get("_cgTopics").is_none()
                && let Some(obj) = first.as_object_mut()
            {
                obj.insert("_cgTopics".into(), Value::String("-".into()));
                obj.insert("_cgPartitions".into(), Value::Number(0.into()));
                obj.insert("_cgTotalLag".into(), Value::Number(0.into()));
                obj.insert("_cgMaxLag".into(), Value::Number(0.into()));
            }
            Ok(first)
        } else {
            Err(ApiError {
                message: format!("ConsumerGroup '{group}' not found in '{namespace}'"),
                failure: PollFailure::Server,
                status: Some(404),
            })
        }
    }

    async fn reset_user_password(&self, namespace: &str, user: &str) -> Result<Value, ApiError> {
        let url = format!(
            "/api/namespaces/{}/users/{}/reset-password",
            urlencoding::encode(namespace),
            urlencoding::encode(user)
        );
        let (_, _, text) = self.send_authed(reqwest::Method::POST, &url, None).await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    async fn vault_passwords(
        &self,
        namespace: &str,
        connect_cluster: &str,
        passwords: &[String],
    ) -> Result<Vec<Value>, ApiError> {
        let url = format!(
            "/api/namespaces/{}/connect-clusters/{}/vaults",
            urlencoding::encode(namespace),
            urlencoding::encode(connect_cluster)
        );
        let body = json!(passwords);
        let (_, _, text) = self
            .send_authed(reqwest::Method::POST, &url, Some(&body))
            .await?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }
}

/// Expand `ResourceQuotaResponse` items into individual metric rows so the
/// `ResourceQuota` table displays `NAME | KEY | VALUE | USED | AGE` per quota metric.
pub fn expand_quota_rows(kind: &str, list: Vec<Value>) -> Vec<Value> {
    if kind != "ResourceQuota" {
        return list;
    }
    let fields = [
        ("count/topics", "/spec/countTopic"),
        ("count/partitions", "/spec/countPartition"),
        ("disk/topics", "/spec/diskTopic"),
        ("count/connectors", "/spec/countConnector"),
        ("user/producer_byte_rate", "/spec/producerByteRate"),
        ("user/consumer_byte_rate", "/spec/consumerByteRate"),
    ];
    let mut expanded = Vec::new();
    for item in list {
        let mut matched_any = false;
        for (key_label, ptr) in fields {
            if let Some(raw) = item.pointer(ptr).and_then(Value::as_str)
                && !raw.is_empty()
            {
                matched_any = true;
                let mut row = item.clone();
                let (used, val) = raw
                    .split_once('/')
                    .map(|(u, v)| (u.to_string(), v.to_string()))
                    .unwrap_or_else(|| ("-".into(), raw.to_string()));
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("_quotaKey".into(), Value::String(key_label.into()));
                    obj.insert("_quotaValue".into(), Value::String(val));
                    obj.insert("_quotaUsed".into(), Value::String(used));
                }
                expanded.push(row);
            }
        }
        if !matched_any {
            expanded.push(item);
        }
    }
    expanded
}

/// Aggregate raw ns4kafka `ConsumerGroup` partition items into one summary object per group.
///
/// When `ns4kafka` returns stub items (`status.topic == null` because >1 owned group matched
/// `name=*`), `_cgTopics` is omitted so the poller knows to hydrate that group in the background
/// via `GET /api/namespaces/{ns}/consumer-groups?name={groupId}`.
/// When partition offsets are present (single-group query or `/consumer-groups/_/external`),
/// computes `_cgTopics`, `_cgPartitions`, `_cgTotalLag`, `_cgMaxLag`, and `_cgItems`.
pub fn aggregate_consumer_groups(raw_items: Vec<Value>, ownership: &str) -> Vec<Value> {
    let mut order: Vec<String> = Vec::new();
    let mut by_name: HashMap<String, Vec<Value>> = HashMap::new();
    for item in raw_items {
        let name = item
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>")
            .to_string();
        if !by_name.contains_key(&name) {
            order.push(name.clone());
        }
        by_name.entry(name).or_default().push(item);
    }

    let mut out = Vec::with_capacity(order.len());
    for name in order {
        let Some(items) = by_name.remove(&name) else {
            continue;
        };
        let first = items[0].clone();
        let metadata = first.get("metadata").cloned().unwrap_or_else(|| json!({}));
        let state = first
            .pointer("/status/state")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();

        let mut topics: Vec<String> = Vec::new();
        let mut partition_items: Vec<Value> = Vec::new();
        let mut total_lag = 0i64;
        let mut max_lag = 0i64;

        for mut item in items {
            if let Some(topic) = item
                .pointer("/status/topic")
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                if !topics.contains(&topic) {
                    topics.push(topic);
                }
                let lag = item
                    .pointer("/status/lag")
                    .and_then(Value::as_i64)
                    .unwrap_or(0);
                total_lag += lag;
                if lag > max_lag {
                    max_lag = lag;
                }
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("kind".into(), Value::String("ConsumerGroupMember".into()));
                }
                partition_items.push(item);
            }
        }

        let mut group_obj = json!({
            "apiVersion": "v1",
            "kind": "ConsumerGroup",
            "metadata": metadata,
            "status": {
                "state": state,
            },
            "_cgOwnership": ownership,
        });

        if !partition_items.is_empty()
            && let Some(map) = group_obj.as_object_mut()
        {
            map.insert("_cgTopics".into(), Value::String(topics.join(",")));
            map.insert(
                "_cgPartitions".into(),
                Value::Number((partition_items.len() as i64).into()),
            );
            map.insert("_cgTotalLag".into(), Value::Number(total_lag.into()));
            map.insert("_cgMaxLag".into(), Value::Number(max_lag.into()));
            map.insert("_cgItems".into(), Value::Array(partition_items));
        }
        out.push(group_obj);
    }
    out
}

// ---------------------------------------------------------------------------
// FakeClient (sofka's Cluster::fake() equivalent for tests, --demo & --snapshot)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FakeClient {
    ctx: KafkaContext,
    state: Arc<RwLock<FakeState>>,
}

struct FakeState {
    /// Resources keyed by `path` -> `Vec<Value>`
    by_path: HashMap<String, Vec<Value>>,
    /// Namespaces that return 403 Forbidden
    forbidden_namespaces: HashSet<String>,
    /// Unsynchronized resources available for import
    unsynced_by_path: HashMap<String, Vec<Value>>,
}

impl FakeClient {
    pub fn demo(ctx: Option<KafkaContext>) -> Self {
        let ctx = ctx.unwrap_or(KafkaContext {
            name: "clermont-dev".into(),
            api: "https://ns4kafka.clermont.example.com".into(),
            user_token: "glpat-demo-token".into(),
            username: "michelin-dev".into(),
            namespace: "team-orders".into(),
            readonly: false,
        });

        let mut by_path: HashMap<String, Vec<Value>> = HashMap::new();

        by_path.insert(
            "namespaces".into(),
            vec![
                json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "team-orders",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-09-01T08:00:00Z"
                    },
                    "spec": {
                        "kafkaUser": "u-team-orders",
                        "protectionEnabled": true,
                        "transactionsEnabled": true,
                        "connectClusters": ["connect-eu-west"]
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "team-payments",
                        "namespace": "team-payments",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-09-10T10:00:00Z"
                    },
                    "spec": {
                        "kafkaUser": "u-team-payments",
                        "protectionEnabled": false,
                        "transactionsEnabled": false,
                        "connectClusters": ["connect-eu-west"]
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "restricted-audit",
                        "namespace": "restricted-audit",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-08-15T12:00:00Z"
                    },
                    "spec": {
                        "kafkaUser": "u-restricted-audit",
                        "protectionEnabled": true
                    }
                }),
            ],
        );

        by_path.insert(
            "topics".into(),
            vec![
                json!({
                    "apiVersion": "v1",
                    "kind": "Topic",
                    "metadata": {
                        "name": "orders.created.v1",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "generation": 3,
                        "creationTimestamp": "2026-10-01T10:00:00Z",
                        "status": { "phase": "Success" }
                    },
                    "spec": {
                        "partitions": 12,
                        "replicationFactor": 3,
                        "tags": ["CORE", "EVENTS"],
                        "description": "Order creation domain events",
                        "configs": {
                            "cleanup.policy": "delete",
                            "retention.ms": "604800000",
                            "min.insync.replicas": "2"
                        }
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Topic",
                    "metadata": {
                        "name": "orders.dlq.v1",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "generation": 1,
                        "creationTimestamp": "2026-10-12T14:30:00Z",
                        "status": {
                            "phase": "Fail",
                            "message": "Broker rejected replicationFactor=5: only 3 brokers available in cluster puy-de-dome"
                        }
                    },
                    "spec": {
                        "partitions": 3,
                        "replicationFactor": 5,
                        "tags": ["DLQ"],
                        "description": "Dead-letter queue for failed order processing",
                        "configs": {
                            "cleanup.policy": "delete",
                            "retention.ms": "1209600000"
                        }
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Topic",
                    "metadata": {
                        "name": "orders.changelog.v1",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "generation": 2,
                        "creationTimestamp": "2026-10-14T20:00:00Z",
                        "status": {
                            "phase": "Pending",
                            "message": "Awaiting processing by executor"
                        }
                    },
                    "spec": {
                        "partitions": 6,
                        "replicationFactor": 3,
                        "tags": ["COMPACTED"],
                        "configs": {
                            "cleanup.policy": "compact",
                            "retention.ms": "86400000"
                        }
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Topic",
                    "metadata": {
                        "name": "payments.settled.v1",
                        "namespace": "team-payments",
                        "cluster": "puy-de-dome",
                        "generation": 1,
                        "creationTimestamp": "2026-10-05T09:00:00Z",
                        "status": { "phase": "Success" }
                    },
                    "spec": {
                        "partitions": 6,
                        "replicationFactor": 3,
                        "tags": ["PCI"],
                        "configs": {
                            "cleanup.policy": "delete",
                            "retention.ms": "2592000000"
                        }
                    }
                }),
            ],
        );

        by_path.insert(
            "connectors".into(),
            vec![
                json!({
                    "apiVersion": "v1",
                    "kind": "Connector",
                    "metadata": {
                        "name": "orders.postgres-sink",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-10-02T11:00:00Z",
                        "status": { "phase": "Success" }
                    },
                    "spec": {
                        "connectCluster": "connect-eu-west",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                            "topics": "orders.created.v1",
                            "tasks.max": "2",
                            "connection.url": "jdbc:postgresql://db.internal:5432/orders"
                        }
                    },
                    "status": {
                        "state": "RUNNING",
                        "workerId": "connect-worker-0:8083",
                        "tasks": [
                            { "id": "0", "state": "RUNNING", "workerId": "connect-worker-0:8083" },
                            { "id": "1", "state": "RUNNING", "workerId": "connect-worker-1:8083" }
                        ]
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Connector",
                    "metadata": {
                        "name": "orders.s3-archive",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-10-10T16:00:00Z",
                        "status": { "phase": "Success" }
                    },
                    "spec": {
                        "connectCluster": "connect-eu-west",
                        "config": {
                            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                            "topics": "orders.created.v1,orders.dlq.v1",
                            "tasks.max": "2",
                            "s3.bucket.name": "orders-audit-archive"
                        }
                    },
                    "status": {
                        "state": "FAILED",
                        "workerId": "connect-worker-1:8083",
                        "tasks": [
                            { "id": "0", "state": "RUNNING", "workerId": "connect-worker-0:8083" },
                            {
                                "id": "1",
                                "state": "FAILED",
                                "workerId": "connect-worker-1:8083",
                                "trace": "org.apache.kafka.connect.errors.ConnectException: S3 bucket 'orders-audit-archive' returned 403 AccessDenied\n\tat io.confluent.connect.s3.S3SinkTask.put(S3SinkTask.java:214)"
                            }
                        ]
                    }
                }),
            ],
        );

        by_path.insert(
            "consumer-groups".into(),
            vec![
                json!({
                    "apiVersion": "v1",
                    "kind": "ConsumerGroup",
                    "metadata": {
                        "name": "orders.billing-worker",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome"
                    },
                    "status": {
                        "state": "STABLE",
                        "topic": "orders.created.v1",
                        "partition": 0,
                        "currentOffset": 14820,
                        "logEndOffset": 14820,
                        "lag": 0
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConsumerGroup",
                    "metadata": {
                        "name": "orders.billing-worker",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome"
                    },
                    "status": {
                        "state": "STABLE",
                        "topic": "orders.created.v1",
                        "partition": 1,
                        "currentOffset": 14100,
                        "logEndOffset": 14550,
                        "lag": 450
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConsumerGroup",
                    "metadata": {
                        "name": "orders.legacy-consumer",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome"
                    },
                    "status": {
                        "state": "EMPTY",
                        "topic": "orders.dlq.v1",
                        "partition": 0,
                        "currentOffset": 12,
                        "logEndOffset": 50,
                        "lag": 38
                    }
                }),
            ],
        );

        by_path.insert(
            "consumer-groups/_/external".into(),
            vec![json!({
                "apiVersion": "v1",
                "kind": "ConsumerGroup",
                "metadata": {
                    "name": "payments.order-reader",
                    "namespace": "team-orders",
                    "cluster": "puy-de-dome"
                },
                "status": {
                    "state": "STABLE",
                    "topic": "orders.created.v1",
                    "partition": 0,
                    "currentOffset": 14800,
                    "logEndOffset": 14820,
                    "lag": 20
                }
            })],
        );

        by_path.insert(
            "schemas".into(),
            vec![
                json!({
                    "apiVersion": "v1",
                    "kind": "Schema",
                    "metadata": {
                        "name": "orders.created.v1-value",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-10-01T10:05:00Z"
                    },
                    "spec": {
                        "id": 1042,
                        "version": 3,
                        "schemaType": "AVRO",
                        "compatibility": "BACKWARD",
                        "schema": "{\"type\":\"record\",\"name\":\"OrderCreated\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"}]}"
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "Schema",
                    "metadata": {
                        "name": "orders.dlq.v1-value",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome",
                        "creationTimestamp": "2026-10-12T14:35:00Z"
                    },
                    "spec": {
                        "id": 1089,
                        "version": 1,
                        "schemaType": "JSON",
                        "compatibility": "FULL",
                        "schema": "{\"type\":\"object\"}"
                    }
                }),
            ],
        );

        by_path.insert(
            "acls".into(),
            vec![
                json!({
                    "apiVersion": "v1",
                    "kind": "AccessControlEntry",
                    "metadata": {
                        "name": "acl-orders-owner",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome"
                    },
                    "spec": {
                        "resourceType": "TOPIC",
                        "resource": "orders.",
                        "resourcePatternType": "PREFIXED",
                        "permission": "OWNER",
                        "grantedTo": "team-orders"
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "AccessControlEntry",
                    "metadata": {
                        "name": "acl-orders-read-payments",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome"
                    },
                    "spec": {
                        "resourceType": "TOPIC",
                        "resource": "orders.created.v1",
                        "resourcePatternType": "LITERAL",
                        "permission": "READ",
                        "grantedTo": "team-payments"
                    }
                }),
                json!({
                    "apiVersion": "v1",
                    "kind": "AccessControlEntry",
                    "metadata": {
                        "name": "acl-orders-groups",
                        "namespace": "team-orders",
                        "cluster": "puy-de-dome"
                    },
                    "spec": {
                        "resourceType": "GROUP",
                        "resource": "orders.",
                        "resourcePatternType": "PREFIXED",
                        "permission": "OWNER",
                        "grantedTo": "team-orders"
                    }
                }),
            ],
        );

        by_path.insert(
            "streams".into(),
            vec![json!({
                "apiVersion": "v1",
                "kind": "KafkaStream",
                "metadata": {
                    "name": "orders.enrichment-stream",
                    "namespace": "team-orders",
                    "cluster": "puy-de-dome",
                    "creationTimestamp": "2026-10-04T08:00:00Z"
                }
            })],
        );

        by_path.insert(
            "connect-clusters".into(),
            vec![json!({
                "apiVersion": "v1",
                "kind": "ConnectCluster",
                "metadata": {
                    "name": "connect-eu-west",
                    "namespace": "team-orders",
                    "cluster": "puy-de-dome",
                    "creationTimestamp": "2026-09-02T09:00:00Z"
                },
                "spec": {
                    "url": "https://connect-eu-west.internal:8083",
                    "status": "HEALTHY",
                    "aes256Key": "demo-key",
                    "aes256Salt": "demo-salt"
                }
            })],
        );

        by_path.insert(
            "resource-quotas".into(),
            vec![json!({
                "apiVersion": "v1",
                "kind": "ResourceQuotaResponse",
                "metadata": {
                    "name": "quota-team-orders",
                    "namespace": "team-orders",
                    "cluster": "puy-de-dome",
                    "creationTimestamp": "2026-09-01T08:30:00Z"
                },
                "spec": {
                    "countTopic": "3/20",
                    "countPartition": "21/100",
                    "diskTopic": "4.5GiB/50.0GiB",
                    "countConnector": "2/10",
                    "producerByteRate": "5242880",
                    "consumerByteRate": "10485760"
                }
            })],
        );

        by_path.insert(
            "role-bindings".into(),
            vec![json!({
                "apiVersion": "v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": "rb-team-orders-devs",
                    "namespace": "team-orders",
                    "cluster": "puy-de-dome"
                },
                "spec": {
                    "role": {
                        "resourceTypes": ["topics", "connectors", "schemas", "acls", "consumer-groups"],
                        "verbs": ["GET", "POST", "PUT", "DELETE"]
                    },
                    "subject": {
                        "subjectType": "GROUP",
                        "subjectName": "ldap-team-orders"
                    }
                }
            })],
        );

        let mut unsynced_by_path: HashMap<String, Vec<Value>> = HashMap::new();
        unsynced_by_path.insert(
            "topics".into(),
            vec![json!({
                "apiVersion": "v1",
                "kind": "Topic",
                "metadata": {
                    "name": "orders.legacy-imported.v1",
                    "namespace": "team-orders",
                    "cluster": "puy-de-dome",
                    "creationTimestamp": "2026-10-14T21:00:00Z",
                    "status": { "phase": "Success" }
                },
                "spec": {
                    "partitions": 3,
                    "replicationFactor": 3,
                    "tags": ["IMPORTED"],
                    "configs": { "cleanup.policy": "delete", "retention.ms": "604800000" }
                }
            })],
        );

        let mut forbidden_namespaces = HashSet::new();
        forbidden_namespaces.insert("restricted-audit".into());

        Self {
            ctx,
            state: Arc::new(RwLock::new(FakeState {
                by_path,
                forbidden_namespaces,
                unsynced_by_path,
            })),
        }
    }
}

#[async_trait]
impl Ns4KafkaClient for FakeClient {
    fn context(&self) -> &KafkaContext {
        &self.ctx
    }

    async fn login(&self) -> Result<TokenInfo, ApiError> {
        self.token_info().await
    }

    async fn token_info(&self) -> Result<TokenInfo, ApiError> {
        Ok(TokenInfo {
            username: self.ctx.username.clone(),
            active: true,
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            roles: vec!["ROLE_USER".into()],
        })
    }

    async fn server_version(&self) -> Result<String, ApiError> {
        Ok("1.32.0".into())
    }

    async fn api_resources(&self) -> Result<Vec<ResourceDefinition>, ApiError> {
        Ok(DiscoveryRegistry::builtin().resources().to_vec())
    }

    async fn list_namespaces(&self) -> Result<Vec<(String, bool)>, ApiError> {
        let state = self.state.read().unwrap();
        let items = state.by_path.get("namespaces").cloned().unwrap_or_default();
        let mut out = Vec::new();
        for item in items {
            if let Some(name) = item.pointer("/metadata/name").and_then(Value::as_str) {
                let is_forbidden = state.forbidden_namespaces.contains(name);
                out.push((name.to_string(), is_forbidden));
            }
        }
        Ok(out)
    }

    async fn list_resources(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
    ) -> Result<Vec<Value>, ApiError> {
        let state = self.state.read().unwrap();
        if let Some(ns) = namespace
            && state.forbidden_namespaces.contains(ns)
        {
            return Err(ApiError {
                message: format!("HTTP 403: Forbidden access to namespace '{ns}'"),
                failure: PollFailure::Forbidden,
                status: Some(403),
            });
        }

        // Handle ConsumerGroupMember drill-down (`consumer-groups?name=<group>` or `consumer-groups/_/external?name=<group>`)
        if def.kind == "ConsumerGroupMember" {
            let (base_path, name_filter) = def
                .path
                .split_once("?name=")
                .map(|(p, n)| {
                    (
                        p,
                        Some(
                            urlencoding::decode(n)
                                .map(|c| c.into_owned())
                                .unwrap_or_else(|_| n.to_string()),
                        ),
                    )
                })
                .unwrap_or((&def.path, None));
            let items = state.by_path.get(base_path).cloned().unwrap_or_default();
            let filtered: Vec<Value> = items
                .into_iter()
                .filter(|v| {
                    let ns_ok = match namespace {
                        Some(ns) if !ns.is_empty() && ns != "*" => {
                            v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(ns)
                        }
                        _ => true,
                    };
                    let name_ok = match &name_filter {
                        Some(want) => {
                            v.pointer("/metadata/name").and_then(Value::as_str)
                                == Some(want.as_str())
                        }
                        None => true,
                    };
                    ns_ok && name_ok
                })
                .map(|mut v| {
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert("kind".into(), Value::String("ConsumerGroupMember".into()));
                    }
                    v
                })
                .collect();
            return Ok(filtered);
        }

        if def.kind == "ConsumerGroup" {
            let owned_items: Vec<Value> = state
                .by_path
                .get("consumer-groups")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|v| match namespace {
                    Some(ns) if !ns.is_empty() && ns != "*" => {
                        v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(ns)
                    }
                    _ => true,
                })
                .collect();
            let mut groups = aggregate_consumer_groups(owned_items, "Owned");
            // Mirror ns4kafka ConsumerGroupService.findByWildcardName: when >1 owned group matches `name=*`,
            // the list response returns only `(name, state)` without partition offsets (hydrated in background).
            if groups.len() > 1 {
                for g in &mut groups {
                    if let Some(obj) = g.as_object_mut() {
                        obj.remove("_cgTopics");
                        obj.remove("_cgPartitions");
                        obj.remove("_cgTotalLag");
                        obj.remove("_cgMaxLag");
                        obj.remove("_cgItems");
                    }
                }
            }

            let ext_items: Vec<Value> = state
                .by_path
                .get("consumer-groups/_/external")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|v| match namespace {
                    Some(ns) if !ns.is_empty() && ns != "*" => {
                        v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(ns)
                    }
                    _ => true,
                })
                .collect();
            groups.extend(aggregate_consumer_groups(ext_items, "External"));
            return Ok(groups);
        }

        if def.kind == "ExternalConsumerGroup" {
            let ext_items: Vec<Value> = state
                .by_path
                .get("consumer-groups/_/external")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|v| match namespace {
                    Some(ns) if !ns.is_empty() && ns != "*" => {
                        v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(ns)
                    }
                    _ => true,
                })
                .collect();
            return Ok(aggregate_consumer_groups(ext_items, "External"));
        }

        let items = state.by_path.get(&def.path).cloned().unwrap_or_default();
        let mut filtered: Vec<Value> = if !def.namespaced || def.path == "namespaces" {
            items
        } else {
            match namespace {
                Some(ns) if !ns.is_empty() && ns != "*" => items
                    .into_iter()
                    .filter(|v| {
                        v.pointer("/metadata/namespace")
                            .and_then(Value::as_str)
                            .is_some_and(|item_ns| item_ns == ns)
                    })
                    .collect(),
                _ => items,
            }
        };
        // Mirror ns4kafka SchemaController.list: when >1 schema matches, list only
        // returns metadata without spec (hydrated one-by-one via get_schema).
        if def.kind == "Schema" && filtered.len() > 1 {
            for item in &mut filtered {
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("spec");
                }
            }
        }
        Ok(expand_quota_rows(&def.kind, filtered))
    }

    async fn apply_resource(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
        body: &Value,
        dryrun: bool,
    ) -> Result<ApplyResult, ApiError> {
        let mut clean = body.clone();
        if let Some(obj) = clean.as_object_mut() {
            obj.remove("_quotaKey");
            obj.remove("_quotaValue");
            obj.remove("_quotaUsed");
        }
        let name = clean
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("unnamed")
            .to_string();
        let ns = clean
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .or(namespace)
            .unwrap_or(&self.ctx.namespace)
            .to_string();

        let mut state = self.state.write().unwrap();
        let list = state.by_path.entry(def.path.clone()).or_default();
        let pos = list.iter().position(|v| {
            v.pointer("/metadata/name").and_then(Value::as_str) == Some(&name)
                && (!def.namespaced
                    || v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(&ns))
        });

        let status = if pos.is_some() { "changed" } else { "created" }.to_string();
        if !dryrun {
            match pos {
                Some(idx) => list[idx] = clean.clone(),
                None => list.push(clean.clone()),
            }
        }
        Ok(ApplyResult {
            status,
            resource: clean,
        })
    }

    async fn delete_resource(
        &self,
        namespace: Option<&str>,
        def: &ResourceDefinition,
        name: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let ns = namespace.unwrap_or(&self.ctx.namespace);
        let mut state = self.state.write().unwrap();
        let list = state.by_path.entry(def.path.clone()).or_default();
        let matching: Vec<Value> = list
            .iter()
            .filter(|v| {
                v.pointer("/metadata/name").and_then(Value::as_str) == Some(name)
                    && (!def.namespaced
                        || v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(ns))
            })
            .cloned()
            .collect();
        if !dryrun {
            list.retain(|v| {
                !(v.pointer("/metadata/name").and_then(Value::as_str) == Some(name)
                    && (!def.namespaced
                        || v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(ns)))
            });
        }
        Ok(matching)
    }

    async fn import_resources(
        &self,
        namespace: &str,
        path: &str,
        _name_glob: &str,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let mut state = self.state.write().unwrap();
        let unsynced: Vec<Value> = state
            .unsynced_by_path
            .get(path)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|v| v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(namespace))
            .collect();
        if !dryrun && !unsynced.is_empty() {
            let list = state.by_path.entry(path.to_string()).or_default();
            for item in &unsynced {
                list.push(item.clone());
            }
            if let Some(u_list) = state.unsynced_by_path.get_mut(path) {
                u_list.retain(|v| {
                    v.pointer("/metadata/namespace").and_then(Value::as_str) != Some(namespace)
                });
            }
        }
        Ok(unsynced)
    }

    async fn delete_records(
        &self,
        namespace: &str,
        topic: &str,
        _dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        Ok(vec![
            json!({
                "apiVersion": "v1",
                "kind": "DeleteRecordsResponse",
                "metadata": { "name": topic, "namespace": namespace },
                "spec": { "topic": topic, "partition": 0, "offset": 14820 }
            }),
            json!({
                "apiVersion": "v1",
                "kind": "DeleteRecordsResponse",
                "metadata": { "name": topic, "namespace": namespace },
                "spec": { "topic": topic, "partition": 1, "offset": 14550 }
            }),
        ])
    }

    async fn change_connector_state(
        &self,
        namespace: &str,
        connector: &str,
        action: &str,
    ) -> Result<Value, ApiError> {
        let new_state = match action.to_ascii_uppercase().as_str() {
            "PAUSE" => "PAUSED",
            "RESUME" | "RESTART" => "RUNNING",
            "STOP" => "UNASSIGNED",
            _ => "RUNNING",
        };
        let mut state = self.state.write().unwrap();
        if let Some(list) = state.by_path.get_mut("connectors") {
            for item in list.iter_mut() {
                if item.pointer("/metadata/name").and_then(Value::as_str) == Some(connector)
                    && item.pointer("/metadata/namespace").and_then(Value::as_str)
                        == Some(namespace)
                    && let Some(st) = item.get_mut("status").and_then(Value::as_object_mut)
                {
                    st.insert("state".into(), Value::String(new_state.into()));
                    if action.eq_ignore_ascii_case("RESTART")
                        && let Some(tasks) = st.get_mut("tasks").and_then(Value::as_array_mut)
                    {
                        for t in tasks.iter_mut() {
                            if let Some(tobj) = t.as_object_mut() {
                                tobj.insert("state".into(), Value::String("RUNNING".into()));
                                tobj.remove("trace");
                            }
                        }
                    }
                }
            }
        }
        Ok(json!({
            "apiVersion": "v1",
            "kind": "ChangeConnectorState",
            "metadata": { "name": connector, "namespace": namespace },
            "spec": { "action": action },
            "status": { "code": new_state }
        }))
    }

    async fn list_connector_offsets(
        &self,
        namespace: &str,
        connector: &str,
    ) -> Result<Vec<Value>, ApiError> {
        Ok(vec![
            json!({
                "apiVersion": "v1",
                "kind": "ConnectorOffsetResponse",
                "metadata": { "name": connector, "namespace": namespace },
                "spec": {
                    "topic": "orders.created.v1",
                    "partition": 0,
                    "offset": 14820
                }
            }),
            json!({
                "apiVersion": "v1",
                "kind": "ConnectorOffsetResponse",
                "metadata": { "name": connector, "namespace": namespace },
                "spec": {
                    "topic": "orders.created.v1",
                    "partition": 1,
                    "offset": 14550
                }
            }),
        ])
    }

    async fn reset_connector_offsets(
        &self,
        namespace: &str,
        connector: &str,
    ) -> Result<Value, ApiError> {
        Ok(json!({
            "apiVersion": "v1",
            "kind": "ConnectorResetOffsetsResponse",
            "metadata": { "name": connector, "namespace": namespace },
            "status": { "code": "RESET" }
        }))
    }

    async fn reset_consumer_group_offsets(
        &self,
        namespace: &str,
        group: &str,
        topic: &str,
        method: &str,
        _options: Option<&str>,
        dryrun: bool,
    ) -> Result<Vec<Value>, ApiError> {
        let new_offset = if method == "TO_EARLIEST" { 0 } else { 14820 };
        if !dryrun {
            let mut state = self.state.write().unwrap();
            if let Some(list) = state.by_path.get_mut("consumer-groups") {
                for item in list.iter_mut() {
                    if item.pointer("/metadata/name").and_then(Value::as_str) == Some(group)
                        && item.pointer("/metadata/namespace").and_then(Value::as_str)
                            == Some(namespace)
                        && (topic == "*"
                            || item.pointer("/status/topic").and_then(Value::as_str) == Some(topic))
                        && let Some(st) = item.get_mut("status").and_then(Value::as_object_mut)
                    {
                        st.insert("currentOffset".into(), Value::Number(new_offset.into()));
                        st.insert("lag".into(), Value::Number(0.into()));
                    }
                }
            }
        }
        Ok(vec![json!({
            "apiVersion": "v1",
            "kind": "ConsumerGroupResetOffsetsResponse",
            "metadata": { "name": group, "namespace": namespace },
            "spec": {
                "consumerGroup": group,
                "topic": topic,
                "partition": 0,
                "offset": new_offset
            }
        })])
    }

    async fn delete_consumer_group(
        &self,
        namespace: &str,
        group: &str,
        dryrun: bool,
    ) -> Result<(), ApiError> {
        if !dryrun {
            let mut state = self.state.write().unwrap();
            if let Some(list) = state.by_path.get_mut("consumer-groups") {
                list.retain(|v| {
                    !(v.pointer("/metadata/name").and_then(Value::as_str) == Some(group)
                        && v.pointer("/metadata/namespace").and_then(Value::as_str)
                            == Some(namespace))
                });
            }
        }
        Ok(())
    }

    async fn update_schema_compatibility(
        &self,
        namespace: &str,
        subject: &str,
        compatibility: Option<&str>,
    ) -> Result<Value, ApiError> {
        let compat = compatibility.unwrap_or("GLOBAL");
        let mut state = self.state.write().unwrap();
        if let Some(list) = state.by_path.get_mut("schemas") {
            for item in list.iter_mut() {
                if item.pointer("/metadata/name").and_then(Value::as_str) == Some(subject)
                    && item.pointer("/metadata/namespace").and_then(Value::as_str)
                        == Some(namespace)
                    && let Some(spec) = item.get_mut("spec").and_then(Value::as_object_mut)
                {
                    spec.insert("compatibility".into(), Value::String(compat.into()));
                }
            }
        }
        Ok(json!({
            "apiVersion": "v1",
            "kind": "SubjectConfigState",
            "metadata": { "name": subject, "namespace": namespace },
            "spec": { "compatibility": compat }
        }))
    }

    async fn get_schema(&self, namespace: &str, subject: &str) -> Result<Value, ApiError> {
        let state = self.state.read().unwrap();
        let found = state
            .by_path
            .get("schemas")
            .and_then(|list| {
                list.iter().find(|v| {
                    v.pointer("/metadata/name").and_then(Value::as_str) == Some(subject)
                        && v.pointer("/metadata/namespace").and_then(Value::as_str)
                            == Some(namespace)
                })
            })
            .cloned();
        found.ok_or_else(|| ApiError {
            message: format!("Schema '{subject}' not found in '{namespace}'"),
            failure: PollFailure::Server,
            status: Some(404),
        })
    }

    async fn get_consumer_group(&self, namespace: &str, group: &str) -> Result<Value, ApiError> {
        let state = self.state.read().unwrap();
        let matching: Vec<Value> = state
            .by_path
            .get("consumer-groups")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|v| {
                v.pointer("/metadata/name").and_then(Value::as_str) == Some(group)
                    && v.pointer("/metadata/namespace").and_then(Value::as_str) == Some(namespace)
            })
            .collect();
        let mut agg = aggregate_consumer_groups(matching, "Owned");
        if let Some(mut first) = agg.drain(..).next() {
            if first.get("_cgTopics").is_none()
                && let Some(obj) = first.as_object_mut()
            {
                obj.insert("_cgTopics".into(), Value::String("-".into()));
                obj.insert("_cgPartitions".into(), Value::Number(0.into()));
                obj.insert("_cgTotalLag".into(), Value::Number(0.into()));
                obj.insert("_cgMaxLag".into(), Value::Number(0.into()));
            }
            Ok(first)
        } else {
            Err(ApiError {
                message: format!("ConsumerGroup '{group}' not found in '{namespace}'"),
                failure: PollFailure::Server,
                status: Some(404),
            })
        }
    }

    async fn reset_user_password(&self, namespace: &str, user: &str) -> Result<Value, ApiError> {
        Ok(json!({
            "apiVersion": "v1",
            "kind": "KafkaUserResetPassword",
            "metadata": { "name": user, "namespace": namespace },
            "spec": { "newPassword": "generated-kafka-scram-pwd-9x8a7b" }
        }))
    }

    async fn vault_passwords(
        &self,
        namespace: &str,
        connect_cluster: &str,
        passwords: &[String],
    ) -> Result<Vec<Value>, ApiError> {
        Ok(passwords
            .iter()
            .map(|p| {
                json!({
                    "apiVersion": "v1",
                    "kind": "VaultResponse",
                    "metadata": { "name": connect_cluster, "namespace": namespace },
                    "spec": {
                        "clearText": p,
                        "encrypted": format!("${{aes256:{connect_cluster}:encryptedPayload}}")
                    }
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn make_jwt(sub: &str, exp: i64, roles: &[&str]) -> String {
        use base64::Engine as _;
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload_json = json!({
            "sub": sub,
            "exp": exp,
            "roles": roles,
        });
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload_json.to_string());
        format!("{header}.{payload}.signature")
    }

    #[test]
    fn parses_unexpired_jwt_claims_and_rejects_expired_tokens() {
        let now = chrono::Utc::now().timestamp();
        let valid_tok = make_jwt("alice@michelin.com", now + 3600, &["isAdmin()"]);
        let info = parse_jwt_claims(&valid_tok).expect("valid JWT should parse");
        assert_eq!(info.username, "alice@michelin.com");
        assert_eq!(info.roles, vec!["isAdmin()"]);
        assert!(info.active);

        let expired_tok = make_jwt("alice@michelin.com", now - 60, &[]);
        assert!(
            parse_jwt_claims(&expired_tok).is_none(),
            "expired JWT must be rejected"
        );
        assert!(parse_jwt_claims("not-a-jwt").is_none());
    }

    #[test]
    fn parses_ns4kafka_status_error_messages_and_causes() {
        let body = r#"{
            "status": "Failed",
            "message": "Invalid resource",
            "details": {
                "causes": ["Partition count must be >= 1", "Replication factor invalid"]
            }
        }"#;
        let err = ApiError::from_status(StatusCode::UNPROCESSABLE_ENTITY, body);
        assert_eq!(err.failure, PollFailure::Server);
        assert_eq!(err.status, Some(422));
        assert!(err.message.contains("Invalid resource"));
        assert!(
            err.message
                .contains("Partition count must be >= 1; Replication factor invalid")
        );

        let unauth = ApiError::from_status(StatusCode::UNAUTHORIZED, "");
        assert_eq!(unauth.failure, PollFailure::Unauthorized);

        let forb = ApiError::from_status(StatusCode::FORBIDDEN, "Forbidden");
        assert_eq!(forb.failure, PollFailure::Forbidden);
    }

    #[tokio::test]
    async fn http_client_relogs_on_401_and_executes_rest_endpoints_against_mock_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let valid_jwt = make_jwt(
            "mock-user",
            chrono::Utc::now().timestamp() + 3600,
            &["ROLE_USER"],
        );
        let server_jwt = valid_jwt.clone();

        // Spawn a lightweight mock ns4kafka HTTP server handling:
        // 1. First GET /api/app-info/version with stale token -> 401 Unauthorized
        // 2. POST /login -> 200 OK with fresh JWT
        // 3. Second GET /api/app-info/version -> 200 OK `{"version":"1.33.0"}`
        // 4. GET /api/namespaces/team-orders/schemas/orders-value -> 200 OK `[Schema]`
        // 5. POST /api/namespaces/team-orders/topics?dryrun=true -> 200 OK with X-Ns4kafka-Result header
        let server = tokio::spawn(async move {
            let mut saw_first_401 = false;
            for _ in 0..5 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                let first_line = req.lines().next().unwrap_or_default();

                let response = if first_line.starts_with("POST /login") {
                    let body = json!({
                        "access_token": server_jwt,
                        "username": "mock-user",
                        "roles": ["ROLE_USER"],
                        "expires_in": 3600
                    })
                    .to_string();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line.starts_with("GET /api/app-info/version") && !saw_first_401 {
                    saw_first_401 = true;
                    "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                } else if first_line.starts_with("GET /api/app-info/version") {
                    let body = r#"{"version":"1.33.0"}"#;
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line
                    .starts_with("GET /api/namespaces/team-orders/schemas/orders-value")
                {
                    let body = json!([{
                        "apiVersion": "v1",
                        "kind": "Schema",
                        "metadata": { "name": "orders-value", "namespace": "team-orders" },
                        "spec": { "id": 777, "version": 4, "compatibility": "FULL" }
                    }])
                    .to_string();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line
                    .starts_with("POST /api/namespaces/team-orders/topics?dryrun=true")
                {
                    let body = json!({
                        "apiVersion": "v1",
                        "kind": "Topic",
                        "metadata": { "name": "orders.v1", "namespace": "team-orders" },
                        "spec": { "partitions": 6, "replicationFactor": 3 }
                    })
                    .to_string();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Ns4kafka-Result: created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                };
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let ctx = KafkaContext {
            name: format!("mock-ctx-{}", std::process::id()),
            api: format!("http://{addr}"),
            user_token: "glpat-mock-secret".into(),
            username: "gitlab".into(),
            namespace: "team-orders".into(),
            readonly: false,
        };
        let client = HttpClient::new(ctx).unwrap();
        // Seed an unexpired-looking but server-rejected token so first request hits 401 and triggers re-login
        *client.jwt.write().unwrap() = Some(make_jwt(
            "stale",
            chrono::Utc::now().timestamp() + 3600,
            &[],
        ));

        let ver = client.server_version().await.unwrap();
        assert_eq!(ver, "1.33.0");
        assert_eq!(client.token_info().await.unwrap().username, "mock-user");

        // Single-element JSON array returned by /schemas/{subject} unwraps to the Schema object
        let schema = client
            .get_schema("team-orders", "orders-value")
            .await
            .unwrap();
        assert_eq!(schema.pointer("/spec/id"), Some(&json!(777)));

        // Apply dry-run parses X-Ns4kafka-Result header
        let topic_def = DiscoveryRegistry::builtin()
            .find_by_kind("Topic")
            .unwrap()
            .clone();
        let apply_res = client
            .apply_resource(
                Some("team-orders"),
                &topic_def,
                &json!({"metadata":{"name":"orders.v1"}}),
                true,
            )
            .await
            .unwrap();
        assert_eq!(apply_res.status, "created");

        let _ = std::fs::remove_file(jwt_cache_path(&client.context().name));
        server.await.unwrap();
    }
}
