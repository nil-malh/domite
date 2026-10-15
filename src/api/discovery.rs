//! API resource discovery (`GET /api-resources`) and alias registry.

use serde::{Deserialize, Serialize};

/// One resource definition served by `GET /api-resources` (or synthetic).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDefinition {
    pub kind: String,
    pub namespaced: bool,
    pub synchronizable: bool,
    pub path: String,
    pub names: Vec<String>,
}

impl ResourceDefinition {
    pub fn short_name(&self) -> &str {
        self.names
            .iter()
            .min_by_key(|n| n.len())
            .map(String::as_str)
            .unwrap_or(&self.path)
    }

    pub fn is_declarative(&self) -> bool {
        self.kind != "ConsumerGroup" && self.kind != "KafkaStream"
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryRegistry {
    resources: Vec<ResourceDefinition>,
}

impl Default for DiscoveryRegistry {
    fn default() -> Self {
        Self::builtin()
    }
}

impl DiscoveryRegistry {
    pub fn builtin() -> Self {
        Self::from_server_list(default_resource_definitions())
    }

    pub fn from_server_list(mut list: Vec<ResourceDefinition>) -> Self {
        if !list.iter().any(|r| r.kind == "ConsumerGroup") {
            list.push(consumer_group_definition());
        }
        if !list.iter().any(|r| r.kind == "Namespace") {
            list.push(namespace_definition());
        }
        list.sort_by_key(|r| kind_order(&r.kind));
        Self { resources: list }
    }

    pub fn resources(&self) -> &[ResourceDefinition] {
        &self.resources
    }

    pub fn resolve(&self, query: &str) -> Option<&ResourceDefinition> {
        let q = query.trim().to_ascii_lowercase();
        if q.is_empty() {
            return None;
        }
        self.resources.iter().find(|r| {
            r.kind.eq_ignore_ascii_case(&q)
                || r.path.eq_ignore_ascii_case(&q)
                || r.names.iter().any(|n| n.eq_ignore_ascii_case(&q))
        })
    }

    pub fn find_by_kind(&self, kind: &str) -> Option<&ResourceDefinition> {
        self.resources
            .iter()
            .find(|r| r.kind.eq_ignore_ascii_case(kind))
    }
}

fn kind_order(kind: &str) -> usize {
    match kind {
        "Topic" => 0,
        "Connector" => 1,
        "ConsumerGroup" => 2,
        "Schema" => 3,
        "AccessControlEntry" => 4,
        "KafkaStream" => 5,
        "ConnectCluster" => 6,
        "ResourceQuota" => 7,
        "RoleBinding" => 8,
        "Namespace" => 9,
        _ => 99,
    }
}

pub fn consumer_group_definition() -> ResourceDefinition {
    ResourceDefinition {
        kind: "ConsumerGroup".into(),
        namespaced: true,
        synchronizable: false,
        path: "consumer-groups".into(),
        names: vec![
            "consumer-groups".into(),
            "consumergroups".into(),
            "consumer-group".into(),
            "consumergroup".into(),
            "groups".into(),
            "group".into(),
            "cg".into(),
        ],
    }
}

pub fn namespace_definition() -> ResourceDefinition {
    ResourceDefinition {
        kind: "Namespace".into(),
        namespaced: false,
        synchronizable: false,
        path: "namespaces".into(),
        names: vec!["namespaces".into(), "namespace".into(), "ns".into()],
    }
}

pub fn default_resource_definitions() -> Vec<ResourceDefinition> {
    vec![
        ResourceDefinition {
            kind: "Topic".into(),
            namespaced: true,
            synchronizable: true,
            path: "topics".into(),
            names: vec!["topics".into(), "topic".into(), "to".into()],
        },
        ResourceDefinition {
            kind: "Connector".into(),
            namespaced: true,
            synchronizable: true,
            path: "connectors".into(),
            names: vec![
                "connects".into(),
                "connect".into(),
                "co".into(),
                "connector".into(),
                "connectors".into(),
            ],
        },
        consumer_group_definition(),
        ResourceDefinition {
            kind: "Schema".into(),
            namespaced: true,
            synchronizable: false,
            path: "schemas".into(),
            names: vec!["schemas".into(), "schema".into(), "sc".into()],
        },
        ResourceDefinition {
            kind: "AccessControlEntry".into(),
            namespaced: true,
            synchronizable: false,
            path: "acls".into(),
            names: vec!["acls".into(), "acl".into(), "ac".into()],
        },
        ResourceDefinition {
            kind: "KafkaStream".into(),
            namespaced: true,
            synchronizable: false,
            path: "streams".into(),
            names: vec![
                "streams".into(),
                "stream".into(),
                "st".into(),
                "kstream".into(),
                "kstreams".into(),
                "ks".into(),
            ],
        },
        ResourceDefinition {
            kind: "ConnectCluster".into(),
            namespaced: true,
            synchronizable: false,
            path: "connect-clusters".into(),
            names: vec![
                "connect-clusters".into(),
                "connect-cluster".into(),
                "cc".into(),
                "kconnect".into(),
                "kconnects".into(),
                "kc".into(),
            ],
        },
        ResourceDefinition {
            kind: "ResourceQuota".into(),
            namespaced: true,
            synchronizable: false,
            path: "resource-quotas".into(),
            names: vec![
                "resource-quotas".into(),
                "resource-quota".into(),
                "quotas".into(),
                "quota".into(),
                "qu".into(),
            ],
        },
        ResourceDefinition {
            kind: "RoleBinding".into(),
            namespaced: true,
            synchronizable: false,
            path: "role-bindings".into(),
            names: vec!["rolebindings".into(), "rolebinding".into(), "rb".into()],
        },
        namespace_definition(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_all_short_names_and_kinds() {
        let reg = DiscoveryRegistry::builtin();
        for (alias, expected_kind) in [
            ("to", "Topic"),
            ("topics", "Topic"),
            ("Topic", "Topic"),
            ("co", "Connector"),
            ("connect", "Connector"),
            ("sc", "Schema"),
            ("ac", "AccessControlEntry"),
            ("acl", "AccessControlEntry"),
            ("cg", "ConsumerGroup"),
            ("consumer-groups", "ConsumerGroup"),
            ("st", "KafkaStream"),
            ("ks", "KafkaStream"),
            ("rb", "RoleBinding"),
            ("qu", "ResourceQuota"),
            ("quota", "ResourceQuota"),
            ("cc", "ConnectCluster"),
            ("kc", "ConnectCluster"),
            ("ns", "Namespace"),
        ] {
            let found = reg
                .resolve(alias)
                .unwrap_or_else(|| panic!("failed to resolve {alias}"));
            assert_eq!(found.kind, expected_kind);
        }
    }
}
