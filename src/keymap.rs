// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Built-in actions and their effective keyboard bindings.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::keys::KeyChord;

macro_rules! actions {
    ($($variant:ident => ($name:literal, $description:literal)),* $(,)?) => {
        #[allow(clippy::enum_variant_names)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        pub enum Action { $($variant),* }
        impl Action {
            pub fn name(self) -> &'static str {
                match self { $(Self::$variant => $name),* }
            }
            pub fn description(self) -> &'static str {
                match self { $(Self::$variant => $description),* }
            }
        }
    };
}

actions! {
    Accept => ("accept", "accept selection or input"),
    ActionMenu => ("action_menu", "resource action menu (connector/topic/schema/group)"),
    AllNamespaces => ("all_namespaces", "all namespaces (admin)"),
    Back => ("back", "back or clear search"),
    Backspace => ("backspace", "backspace"),
    ClearLine => ("clear_line", "clear line"),
    Close => ("close", "close view"),
    Command => ("command", "command palette"),
    Complete => ("complete", "fill highlighted suggestion"),
    Contexts => ("contexts", "switch context"),
    Copy => ("copy", "copy content"),
    CopyName => ("copy_name", "copy resource name"),
    Delete => ("delete", "delete with dry-run preview"),
    DeleteWord => ("delete_word", "delete word"),
    Describe => ("describe", "describe resource"),
    Down => ("down", "move cursor down"),
    Edit => ("edit", "edit in $EDITOR with dry-run preview"),
    Exit => ("exit", "quit from table"),
    Explain => ("explain", "explain unhealthy status or connector task traces"),
    Filter => ("filter", "filter rows"),
    First => ("first", "first row"),
    Help => ("help", "help overlay"),
    HistoryBack => ("history_back", "previous view in history"),
    HistoryForward => ("history_forward", "next view in history"),
    Import => ("import", "import unsynchronized topics/connectors"),
    InvertSort => ("invert_sort", "invert sort order"),
    Last => ("last", "last row"),
    Left => ("left", "scroll left"),
    Mark => ("mark", "mark/unmark row for bulk action"),
    Namespaces => ("namespaces", "switch namespace"),
    Open => ("open", "drill down into selection"),
    PageDown => ("page_down", "page down"),
    PageUp => ("page_up", "page up"),
    Quit => ("quit", "quit immediately"),
    Refresh => ("refresh", "force poll refresh"),
    RestartConnector => ("restart_connector", "restart connector"),
    Right => ("right", "scroll right"),
    Skins => ("skins", "switch color skin"),
    Sort => ("sort", "open sort column picker"),
    SortAge => ("sort_age", "sort by age"),
    Up => ("up", "move cursor up"),
    Wide => ("wide", "toggle wide columns"),
    Wrap => ("wrap", "toggle line wrap"),
    Yaml => ("yaml", "view resource as YAML")
}

#[derive(Debug, Clone, Copy)]
pub struct KeyInput {
    pub action: Option<Action>,
    pub code: KeyCode,
    pub modifiers: KeyModifiers,
}

impl KeyInput {
    pub fn new(action: Option<Action>, key: KeyEvent) -> Self {
        Self {
            action,
            code: key.code,
            modifiers: key.modifiers,
        }
    }

    pub fn is_text(self) -> bool {
        self.modifiers.contains(KeyModifiers::CONTROL) == self.modifiers.contains(KeyModifiers::ALT)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keymap {
    bindings: BTreeMap<&'static str, BTreeMap<Action, Vec<KeyChord>>>,
}

const GLOBAL: &[(Action, &[&str])] = &[(Action::Quit, &["ctrl-c"])];

const INPUT: &[(Action, &[&str])] = &[
    (Action::Back, &["esc"]),
    (Action::Backspace, &["backspace", "ctrl-h"]),
    (Action::Accept, &["enter"]),
    (Action::ClearLine, &["ctrl-u"]),
    (
        Action::DeleteWord,
        &["ctrl-w", "alt-backspace", "ctrl-backspace"],
    ),
];

const VIEW_SCOPES: &[&str] = &[
    "table",
    "detail",
    "diff",
    "explain",
    "help",
    "confirm",
    "action_menu",
    "skins",
];

const TEXT_SCOPES: &[&str] = &[
    "command",
    "filter",
    "prompt",
    "sort_picker",
    "namespaces",
    "contexts",
];

const DEFAULTS: &[(&str, Action, &[&str])] = &[
    ("table", Action::Open, &["enter"]),
    ("table", Action::Back, &["esc"]),
    ("table", Action::Exit, &["q"]),
    ("table", Action::Down, &["j", "down"]),
    ("table", Action::Up, &["k", "up"]),
    ("table", Action::First, &["g", "home"]),
    ("table", Action::Last, &["G", "end"]),
    ("table", Action::PageDown, &["pagedown", "ctrl-f"]),
    ("table", Action::PageUp, &["pageup", "ctrl-b"]),
    ("table", Action::Filter, &["/"]),
    ("table", Action::Command, &[":"]),
    ("table", Action::Help, &["?"]),
    ("table", Action::Namespaces, &["n"]),
    ("table", Action::Contexts, &["C"]),
    ("table", Action::AllNamespaces, &["0"]),
    ("table", Action::Yaml, &["y"]),
    ("table", Action::Describe, &["d"]),
    ("table", Action::Edit, &["e"]),
    ("table", Action::Delete, &["ctrl-d"]),
    ("table", Action::ActionMenu, &["t"]),
    ("table", Action::Explain, &["X"]),
    ("table", Action::Mark, &["space"]),
    ("table", Action::Sort, &["S"]),
    ("table", Action::InvertSort, &["I"]),
    ("table", Action::SortAge, &["A"]),
    ("table", Action::Refresh, &["r", "ctrl-r"]),
    ("table", Action::Import, &["i"]),
    ("table", Action::Skins, &["K"]),
    ("table", Action::Wide, &["w"]),
    ("table", Action::CopyName, &["c"]),
    ("table", Action::HistoryBack, &["["]),
    ("table", Action::HistoryForward, &["]"]),
    // Detail / YAML
    ("detail", Action::Back, &["esc"]),
    ("detail", Action::Close, &["q"]),
    ("detail", Action::Down, &["j", "down"]),
    ("detail", Action::Up, &["k", "up"]),
    ("detail", Action::First, &["g", "home"]),
    ("detail", Action::Last, &["G", "end"]),
    ("detail", Action::PageDown, &["pagedown", "space", "ctrl-f"]),
    ("detail", Action::PageUp, &["pageup", "ctrl-b"]),
    ("detail", Action::Left, &["h", "left"]),
    ("detail", Action::Right, &["l", "right"]),
    ("detail", Action::Wrap, &["w"]),
    ("detail", Action::Edit, &["e"]),
    ("detail", Action::Copy, &["c"]),
    // Diff / Dry-run preview
    ("diff", Action::Accept, &["y", "Y", "enter"]),
    ("diff", Action::Back, &["esc", "n", "N", "q"]),
    ("diff", Action::Down, &["j", "down"]),
    ("diff", Action::Up, &["k", "up"]),
    ("diff", Action::First, &["g", "home"]),
    ("diff", Action::Last, &["G", "end"]),
    ("diff", Action::PageDown, &["pagedown", "space", "ctrl-f"]),
    ("diff", Action::PageUp, &["pageup", "ctrl-b"]),
    // Explain
    ("explain", Action::Back, &["esc"]),
    ("explain", Action::Close, &["q"]),
    ("explain", Action::Down, &["j", "down"]),
    ("explain", Action::Up, &["k", "up"]),
    ("explain", Action::First, &["g", "home"]),
    ("explain", Action::Last, &["G", "end"]),
    (
        "explain",
        Action::PageDown,
        &["pagedown", "space", "ctrl-f"],
    ),
    ("explain", Action::PageUp, &["pageup", "ctrl-b"]),
    ("explain", Action::Wrap, &["w"]),
    // Help
    ("help", Action::Back, &["esc"]),
    ("help", Action::Close, &["q", "?"]),
    ("help", Action::Down, &["j", "down"]),
    ("help", Action::Up, &["k", "up"]),
    ("help", Action::First, &["g", "home"]),
    ("help", Action::Last, &["G", "end"]),
    ("help", Action::PageDown, &["pagedown", "space", "ctrl-f"]),
    ("help", Action::PageUp, &["pageup", "ctrl-b"]),
    // Confirm
    ("confirm", Action::Accept, &["y", "Y", "enter"]),
    ("confirm", Action::Back, &["esc", "n", "N", "q"]),
    // Action menu
    ("action_menu", Action::Accept, &["enter"]),
    ("action_menu", Action::Back, &["esc"]),
    ("action_menu", Action::Close, &["q"]),
    ("action_menu", Action::Down, &["j", "down"]),
    ("action_menu", Action::Up, &["k", "up"]),
    // Skins picker
    ("skins", Action::Accept, &["enter"]),
    ("skins", Action::Back, &["esc"]),
    ("skins", Action::Close, &["q"]),
    ("skins", Action::Down, &["j", "down"]),
    ("skins", Action::Up, &["k", "up"]),
    // Command / Filter / Pickers
    ("command", Action::Complete, &["right"]),
    ("command", Action::Down, &["tab", "down", "ctrl-n"]),
    ("command", Action::Up, &["backtab", "up", "ctrl-p"]),
    ("namespaces", Action::Down, &["down", "ctrl-n", "tab"]),
    ("namespaces", Action::Up, &["up", "ctrl-p", "backtab"]),
    ("contexts", Action::Down, &["down", "ctrl-n", "tab"]),
    ("contexts", Action::Up, &["up", "ctrl-p", "backtab"]),
    ("sort_picker", Action::Down, &["down", "ctrl-n", "tab", "j"]),
    ("sort_picker", Action::Up, &["up", "ctrl-p", "backtab", "k"]),
    ("sort_picker", Action::Close, &["q"]),
];

impl Default for Keymap {
    fn default() -> Self {
        static DEFAULT: OnceLock<Keymap> = OnceLock::new();
        DEFAULT
            .get_or_init(|| {
                let mut bindings: BTreeMap<_, BTreeMap<_, _>> = BTreeMap::new();
                for &(scope, action, chords) in DEFAULTS {
                    bindings.entry(scope).or_default().insert(
                        action,
                        chords
                            .iter()
                            .map(|s| KeyChord::parse(s).expect("valid default key"))
                            .collect(),
                    );
                }
                for &scope in TEXT_SCOPES {
                    bindings.entry(scope).or_default();
                }
                for (&scope, actions) in &mut bindings {
                    for &(action, chords) in GLOBAL
                        .iter()
                        .chain(INPUT.iter().filter(|_| TEXT_SCOPES.contains(&scope)))
                    {
                        actions.insert(
                            action,
                            chords
                                .iter()
                                .map(|s| KeyChord::parse(s).expect("valid default key"))
                                .collect(),
                        );
                    }
                    if VIEW_SCOPES.contains(&scope) {
                        actions
                            .entry(Action::Command)
                            .or_insert_with(|| vec![KeyChord::parse(":").unwrap()]);
                        if scope != "help" {
                            actions
                                .entry(Action::Help)
                                .or_insert_with(|| vec![KeyChord::parse("?").unwrap()]);
                        }
                    }
                }
                Keymap { bindings }
            })
            .clone()
    }
}

impl Keymap {
    pub fn resolve(&self, scope: &str, event: KeyEvent) -> KeyInput {
        let action = self.bindings.get(scope).and_then(|actions| {
            actions
                .iter()
                .find_map(|(&act, chords)| chords.iter().any(|c| c.matches(&event)).then_some(act))
        });
        KeyInput::new(action, event)
    }

    pub fn label(&self, scope: &str, action: Action) -> String {
        self.bindings
            .get(scope)
            .and_then(|m| m.get(&action))
            .and_then(|chords| chords.first())
            .map(KeyChord::label)
            .unwrap_or_default()
    }
}
