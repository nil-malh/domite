//! Configuration loading for both `~/.kafkactl/config.yml` (contexts & auth)
//! and `~/.config/domite/config.toml` (UI skins, polling cadence, read-only
//! guardrails, and custom JSON-Pointer views).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// A single ns4kafka context loaded from kafkactl's YAML config or domite's config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaContext {
    pub name: String,
    pub api: String,
    pub user_token: String,
    pub username: String,
    pub namespace: String,
    pub readonly: bool,
}

impl KafkaContext {
    /// Default guardrail: any context whose name starts with `prod` (case-insensitive)
    /// defaults to `readonly = true` unless explicitly overridden.
    pub fn default_readonly_for_name(name: &str) -> bool {
        let lower = name.trim().to_ascii_lowercase();
        lower.starts_with("prod") || lower.contains("-prod") || lower.contains("_prod")
    }
}

/// Raw kafkactl YAML root (`~/.kafkactl/config.yml`).
#[derive(Debug, Default, Deserialize)]
struct KafkactlRoot {
    #[serde(default)]
    kafkactl: KafkactlSection,
    #[serde(rename = "current-context", default)]
    current_context: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct KafkactlSection {
    #[serde(default)]
    contexts: Vec<KafkactlContextEntry>,
    #[serde(rename = "current-context", alias = "currentContext", default)]
    current_context: Option<String>,
    #[serde(default)]
    api: Option<String>,
    #[serde(
        rename = "current-namespace",
        alias = "currentNamespace",
        alias = "namespace",
        default
    )]
    current_namespace: Option<String>,
    #[serde(rename = "user-token", alias = "userToken", default)]
    user_token: Option<String>,
    #[serde(default)]
    username: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct KafkactlContextEntry {
    name: String,
    context: KafkactlContextDef,
}

#[derive(Debug, Default, Deserialize)]
struct KafkactlContextDef {
    #[serde(default)]
    api: String,
    #[serde(rename = "user-token", alias = "userToken", default)]
    user_token: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
}

/// Domite's own TOML configuration (`~/.config/domite/config.toml`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DomiteConfig {
    #[serde(default)]
    pub skin: SkinConfig,
    /// Poll interval in seconds for the active resource view (default: 5).
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Global read-only mode.
    #[serde(default)]
    pub readonly: Option<bool>,
    /// Minimum expected ns4kafka version (optional warning check).
    #[serde(default)]
    pub min_server_version: Option<String>,
    /// Per-context overrides (e.g. `readonly = true`, `skin = "catppuccin-latte"`).
    #[serde(default)]
    pub contexts: HashMap<String, ContextOverride>,
    /// Custom per-kind table column views (`[views.topics]`, `[views.connectors]`).
    #[serde(default)]
    pub views: HashMap<String, ViewConfig>,
}

fn default_poll_interval() -> u64 {
    5
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SkinConfig {
    pub name: Option<String>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub colors: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ContextOverride {
    pub readonly: Option<bool>,
    pub skin: Option<String>,
    pub namespace: Option<String>,
    pub api: Option<String>,
    pub user_token: Option<String>,
    pub username: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ViewConfig {
    #[serde(default)]
    pub replace: bool,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default)]
    pub sort_desc: bool,
    #[serde(default)]
    pub columns: Vec<UserColumnConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UserColumnConfig {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub wide: bool,
}

/// Combined runtime configuration resolved from kafkactl + domite config files.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub domite: DomiteConfig,
    pub contexts: Vec<KafkaContext>,
    pub current_context: Option<String>,
    pub warnings: Vec<String>,
}

impl Default for LoadedConfig {
    fn default() -> Self {
        Self {
            domite: DomiteConfig {
                poll_interval_secs: 5,
                ..DomiteConfig::default()
            },
            contexts: Vec::new(),
            current_context: None,
            warnings: Vec::new(),
        }
    }
}

impl LoadedConfig {
    /// Load configuration from standard paths or explicit overrides.
    pub fn load(kafkactl_path: Option<&Path>, domite_path: Option<&Path>) -> Self {
        let mut warnings = Vec::new();

        let domite = match domite_path
            .map(PathBuf::from)
            .or_else(default_domite_config_path)
        {
            Some(path) if path.exists() => match std::fs::read_to_string(&path) {
                Ok(raw) => match toml::from_str::<DomiteConfig>(&raw) {
                    Ok(mut cfg) => {
                        if cfg.poll_interval_secs == 0 {
                            cfg.poll_interval_secs = 5;
                        }
                        cfg
                    }
                    Err(e) => {
                        warnings.push(format!("{}: {e}", path.display()));
                        DomiteConfig {
                            poll_interval_secs: 5,
                            ..DomiteConfig::default()
                        }
                    }
                },
                Err(e) => {
                    warnings.push(format!("{}: {e}", path.display()));
                    DomiteConfig {
                        poll_interval_secs: 5,
                        ..DomiteConfig::default()
                    }
                }
            },
            _ => DomiteConfig {
                poll_interval_secs: 5,
                ..DomiteConfig::default()
            },
        };

        warnings.extend(crate::theme::validate_skin(
            domite.skin.name.as_deref(),
            &domite.skin.colors,
        ));

        let mut contexts = Vec::new();
        let mut current_context = None;

        if let Some(kc_path) = kafkactl_path
            .map(PathBuf::from)
            .or_else(default_kafkactl_config_path)
            && kc_path.exists()
        {
            match std::fs::read_to_string(&kc_path) {
                Ok(raw) => match parse_kafkactl_yaml(&raw, &domite) {
                    Ok((parsed_contexts, cur)) => {
                        contexts = parsed_contexts;
                        current_context = cur;
                    }
                    Err(e) => warnings.push(format!("{}: {e}", kc_path.display())),
                },
                Err(e) => warnings.push(format!("{}: {e}", kc_path.display())),
            }
        }

        for (name, ov) in &domite.contexts {
            if !contexts.iter().any(|c| &c.name == name)
                && let Some(api) = &ov.api
            {
                let readonly = ov
                    .readonly
                    .or(domite.readonly)
                    .unwrap_or_else(|| KafkaContext::default_readonly_for_name(name));
                contexts.push(KafkaContext {
                    name: name.clone(),
                    api: api.clone(),
                    user_token: ov.user_token.clone().unwrap_or_default(),
                    username: ov.username.clone().unwrap_or_else(|| "gitlab".into()),
                    namespace: ov.namespace.clone().unwrap_or_else(|| "default".into()),
                    readonly,
                });
            }
        }

        if current_context.is_none() {
            current_context = contexts.first().map(|c| c.name.clone());
        }

        LoadedConfig {
            domite,
            contexts,
            current_context,
            warnings,
        }
    }

    pub fn find_context(&self, name: &str) -> Option<&KafkaContext> {
        self.contexts.iter().find(|c| c.name == name)
    }
}

pub fn parse_kafkactl_yaml(
    yaml_str: &str,
    domite: &DomiteConfig,
) -> Result<(Vec<KafkaContext>, Option<String>), String> {
    let root: KafkactlRoot = serde_yaml::from_str(yaml_str).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for entry in root.kafkactl.contexts {
        let ov = domite.contexts.get(&entry.name);
        let readonly = ov
            .and_then(|o| o.readonly)
            .or(domite.readonly)
            .unwrap_or_else(|| KafkaContext::default_readonly_for_name(&entry.name));
        let namespace = ov
            .and_then(|o| o.namespace.clone())
            .or(entry.context.namespace)
            .unwrap_or_else(|| "default".into());
        let api = ov.and_then(|o| o.api.clone()).unwrap_or(entry.context.api);
        let user_token = ov
            .and_then(|o| o.user_token.clone())
            .unwrap_or(entry.context.user_token);
        let username = ov
            .and_then(|o| o.username.clone())
            .or(entry.context.username)
            .unwrap_or_else(|| "gitlab".into());

        out.push(KafkaContext {
            name: entry.name,
            api,
            user_token,
            username,
            namespace,
            readonly,
        });
    }
    let mut current = root.kafkactl.current_context.or(root.current_context);

    // kafkactl `use-context` writes `api`, `current-namespace`, and `user-token`
    // at the top of `kafkactl:`. Match those against `contexts` when `current-context`
    // is not explicitly present.
    if current.is_none()
        && let Some(active_api) = &root.kafkactl.api
    {
        let active_ns = root.kafkactl.current_namespace.as_deref();
        current = out
            .iter()
            .find(|c| c.api == *active_api && active_ns.is_none_or(|ns| c.namespace == ns))
            .or_else(|| out.iter().find(|c| c.api == *active_api))
            .map(|c| c.name.clone());

        if current.is_none() {
            let name = "default".to_string();
            let ns = root
                .kafkactl
                .current_namespace
                .unwrap_or_else(|| "default".into());
            let tok = root.kafkactl.user_token.unwrap_or_default();
            let user = root.kafkactl.username.unwrap_or_else(|| "gitlab".into());
            let readonly = domite
                .readonly
                .unwrap_or_else(|| KafkaContext::default_readonly_for_name(&name));
            out.insert(
                0,
                KafkaContext {
                    name: name.clone(),
                    api: active_api.clone(),
                    user_token: tok,
                    username: user,
                    namespace: ns,
                    readonly,
                },
            );
            current = Some(name);
        }
    }

    if current.is_none() {
        current = out.first().map(|c| c.name.clone());
    }
    Ok((out, current))
}

pub fn default_kafkactl_config_path() -> Option<PathBuf> {
    if let Ok(env_path) = std::env::var("KAFKACTL_CONFIG")
        && !env_path.is_empty()
    {
        return Some(PathBuf::from(env_path));
    }
    let home = dirs::home_dir()?;
    let dot_kafkactl = home.join(".kafkactl").join("config.yml");
    if dot_kafkactl.exists() {
        return Some(dot_kafkactl);
    }
    let yaml_alt = home.join(".kafkactl").join("config.yaml");
    if yaml_alt.exists() {
        return Some(yaml_alt);
    }
    if let Some(cfg_dir) = dirs::config_dir() {
        let xdg = cfg_dir.join("kafkactl").join("config.yml");
        if xdg.exists() {
            return Some(xdg);
        }
    }
    Some(dot_kafkactl)
}

pub fn default_domite_config_path() -> Option<PathBuf> {
    if let Ok(env_path) = std::env::var("DOMITE_CONFIG")
        && !env_path.is_empty()
    {
        return Some(PathBuf::from(env_path));
    }
    dirs::config_dir().map(|d| d.join("domite").join("config.toml"))
}

pub fn jwt_cache_path(context_name: &str) -> PathBuf {
    let base = dirs::cache_dir()
        .or_else(dirs::home_dir)
        .unwrap_or_else(std::env::temp_dir)
        .join("domite")
        .join("jwt");
    let safe_name: String = context_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    base.join(format!("{safe_name}.jwt"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kafkactl_contexts_and_sets_prod_readonly_by_default() {
        let yaml = r#"
kafkactl:
  current-context: dev
  contexts:
    - name: dev
      context:
        api: https://ns4kafka-dev-api.domain.com
        user-token: devGitLabToken
        namespace: team-alpha
    - name: prod-eu
      context:
        api: https://ns4kafka-prod-api.domain.com
        user-token: prodGitLabToken
        namespace: team-alpha
"#;
        let domite = DomiteConfig::default();
        let (contexts, current) = parse_kafkactl_yaml(yaml, &domite).unwrap();
        assert_eq!(current.as_deref(), Some("dev"));
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].name, "dev");
        assert!(!contexts[0].readonly);
        assert_eq!(contexts[1].name, "prod-eu");
        assert!(contexts[1].readonly);
    }
}
