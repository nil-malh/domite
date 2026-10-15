// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Data-driven palette + semantic styles for the TUI.

#![allow(dead_code)]

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use ratatui::style::{Color, Modifier, Style};
use terminal_colorsaurus::{QueryOptions, ThemeMode};

macro_rules! palette_swatches {
    ($($idx:expr => $field:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq)]
        pub struct Palette {
            $(pub $field: Color,)+
        }

        const FIELDS: &[&str] = &[$(stringify!($field),)+];

        impl Palette {
            fn from_hexes(h: &[&str]) -> Palette {
                assert_eq!(
                    h.len(),
                    FIELDS.len(),
                    "built-in palette has {} colors but {} swatches are defined",
                    h.len(),
                    FIELDS.len()
                );
                let c = |i: usize| {
                    parse_hex(h[i]).unwrap_or_else(|| panic!("bad built-in hex {}", h[i]))
                };
                Palette {
                    $($field: c($idx),)+
                }
            }

            fn set(&mut self, key: &str, color: Color) -> bool {
                match key {
                    $(stringify!($field) => self.$field = color,)+
                    _ => return false,
                }
                true
            }
        }

        $(pub fn $field() -> Color {
            palette().$field
        })+
    };
}

palette_swatches! {
    0 => rosewater,
    1 => flamingo,
    2 => pink,
    3 => mauve,
    4 => red,
    5 => maroon,
    6 => peach,
    7 => yellow,
    8 => green,
    9 => teal,
    10 => sky,
    11 => sapphire,
    12 => blue,
    13 => lavender,
    14 => text,
    15 => subtext1,
    16 => subtext0,
    17 => overlay1,
    18 => overlay0,
    19 => surface2,
    20 => surface1,
    21 => surface0,
    22 => base,
    23 => mantle,
    24 => crust,
}

pub const BUILTIN_NAMES: &[&str] = &[
    "catppuccin-mocha",
    "catppuccin-latte",
    "catppuccin-frappe",
    "catppuccin-macchiato",
    "gruvbox-dark",
    "gruvbox-light",
    "nord",
    "dracula",
    "solarized-dark",
    "solarized-light",
    "tokyo-night",
    "one-dark",
    "rose-pine",
    "rose-pine-dawn",
    "monokai",
    "flexoki-dark",
    "flexoki-light",
];

pub fn builtin(name: &str) -> Option<Palette> {
    let hex: [&str; 25] = match name {
        "catppuccin-mocha" | "mocha" => [
            "#f5e0dc", "#f2cdcd", "#f5c2e7", "#cba6f7", "#f38ba8", "#eba0ac", "#fab387", "#f9e2af",
            "#a6e3a1", "#94e2d5", "#89dceb", "#74c7ec", "#89b4fa", "#b4befe", "#cdd6f4", "#bac2de",
            "#a6adc8", "#7f849c", "#6c7086", "#585b70", "#45475a", "#313244", "#1e1e2e", "#181825",
            "#11111b",
        ],
        "catppuccin-latte" | "latte" => [
            "#dc8a78", "#dd7878", "#ea76cb", "#8839ef", "#d20f39", "#e64553", "#fe640b", "#df8e1d",
            "#40a02b", "#179299", "#04a5e5", "#209fb5", "#1e66f5", "#7287fd", "#4c4f69", "#5c5f77",
            "#6c6f85", "#8c8fa1", "#9ca0b0", "#acb0be", "#bcc0cc", "#ccd0da", "#eff1f5", "#e6e9ef",
            "#dce0e8",
        ],
        "catppuccin-frappe" | "frappe" => [
            "#f2d5cf", "#eebebe", "#f4b8e4", "#ca9ee6", "#e78284", "#ea999c", "#ef9f76", "#e5c890",
            "#a6d189", "#81c8be", "#99d1db", "#85c1dc", "#8caaee", "#babbf1", "#c6d0f5", "#b5bfe2",
            "#a5adce", "#949cbb", "#838ba7", "#626880", "#51576d", "#414559", "#303446", "#292c3c",
            "#232634",
        ],
        "catppuccin-macchiato" | "macchiato" => [
            "#f4dbd6", "#f0c6c6", "#f5bde6", "#c6a0f6", "#ed8796", "#ee99a0", "#f5a97f", "#eed49f",
            "#a6da95", "#8bd5ca", "#91d7e3", "#7dc4e4", "#8aadf4", "#b7bdf8", "#cad3f5", "#b8c0e0",
            "#a5adcb", "#8087a2", "#6e738d", "#5b6078", "#494d64", "#363a4f", "#24273a", "#1e2030",
            "#181926",
        ],
        "gruvbox" | "gruvbox-dark" => [
            "#ebdbb2", "#d5c4a1", "#d3869b", "#d3869b", "#fb4934", "#cc241d", "#fe8019", "#fabd2f",
            "#b8bb26", "#8ec07c", "#83a598", "#458588", "#83a598", "#d3869b", "#ebdbb2", "#d5c4a1",
            "#bdae93", "#a89984", "#928374", "#665c54", "#504945", "#3c3836", "#282828", "#1d2021",
            "#1d2021",
        ],
        "nord" => [
            "#d8dee9", "#e5e9f0", "#b48ead", "#b48ead", "#bf616a", "#bf616a", "#d08770", "#ebcb8b",
            "#a3be8c", "#8fbcbb", "#88c0d0", "#81a1c1", "#5e81ac", "#b48ead", "#eceff4", "#e5e9f0",
            "#d8dee9", "#616e88", "#4c566a", "#434c5e", "#3b4252", "#333a47", "#2e3440", "#2b303b",
            "#242933",
        ],
        "dracula" => [
            "#f8f8f2", "#ffb86c", "#ff79c6", "#bd93f9", "#ff5555", "#ff5555", "#ffb86c", "#f1fa8c",
            "#50fa7b", "#8be9fd", "#8be9fd", "#62d6e8", "#6272a4", "#bd93f9", "#f8f8f2", "#d8d8d2",
            "#b8b8b2", "#6272a4", "#565761", "#44475a", "#3a3c4e", "#343746", "#282a36", "#21222c",
            "#191a21",
        ],
        "gruvbox-light" => [
            "#ebdbb2", "#d5c4a1", "#b16286", "#b16286", "#cc241d", "#9d0006", "#d65d0e", "#d79921",
            "#98971a", "#689d6a", "#458588", "#076678", "#458588", "#b16286", "#282828", "#3c3836",
            "#504945", "#665c54", "#7c6f64", "#928374", "#a89984", "#bdae93", "#fbf1c7", "#ebdbb2",
            "#d5c4a1",
        ],
        "solarized-dark" | "solarized" => [
            "#d33682", "#d33682", "#d33682", "#6c71c4", "#dc322f", "#dc322f", "#cb4b16", "#b58900",
            "#859900", "#2aa198", "#2aa198", "#268bd2", "#268bd2", "#6c71c4", "#93a1a1", "#839496",
            "#657b83", "#586e75", "#586e75", "#586e75", "#073642", "#073642", "#002b36", "#002b36",
            "#001e26",
        ],
        "solarized-light" => [
            "#d33682", "#d33682", "#d33682", "#6c71c4", "#dc322f", "#dc322f", "#cb4b16", "#b58900",
            "#859900", "#2aa198", "#2aa198", "#268bd2", "#268bd2", "#6c71c4", "#002b36", "#073642",
            "#586e75", "#657b83", "#839496", "#93a1a1", "#eee8d5", "#eee8d5", "#fdf6e3", "#eee8d5",
            "#eee8d5",
        ],
        "tokyo-night" | "tokyonight" => [
            "#f7768e", "#f7768e", "#bb9af7", "#9d7cd8", "#f7768e", "#db4b4b", "#ff9e64", "#e0af68",
            "#9ece6a", "#73daca", "#7dcfff", "#0db9d7", "#7aa2f7", "#9d7cd8", "#c0caf5", "#a9b1d6",
            "#545c7e", "#545c7e", "#3b4261", "#3b4261", "#292e42", "#292e42", "#1a1b26", "#16161e",
            "#13131a",
        ],
        "one-dark" | "onedark" => [
            "#e06c75", "#e06c75", "#c678dd", "#c678dd", "#e06c75", "#be5046", "#d19a66", "#e5c07b",
            "#98c379", "#56b6c2", "#56b6c2", "#61afef", "#61afef", "#c678dd", "#abb2bf", "#828997",
            "#5c6370", "#5c6370", "#4b5263", "#4b5263", "#3b4048", "#323842", "#282c34", "#21252b",
            "#1b1d23",
        ],
        "rose-pine" | "rosepine" => [
            "#ebbcba", "#ebbcba", "#ebbcba", "#c4a7e7", "#eb6f92", "#eb6f92", "#f6c177", "#f6c177",
            "#31748f", "#31748f", "#9ccfd8", "#9ccfd8", "#9ccfd8", "#c4a7e7", "#e0def4", "#908caa",
            "#6e6a86", "#6e6a86", "#524f67", "#524f67", "#403d52", "#26233a", "#191724", "#191724",
            "#14121d",
        ],
        "rose-pine-dawn" | "rosepinedawn" => [
            "#d7827e", "#d7827e", "#d7827e", "#907aa9", "#b4637a", "#b4637a", "#ea9d34", "#ea9d34",
            "#286983", "#286983", "#56949f", "#56949f", "#56949f", "#907aa9", "#575279", "#797593",
            "#9893a5", "#9893a5", "#cecacd", "#cecacd", "#dfdad9", "#f2e9e1", "#faf4ed", "#faf4ed",
            "#f4ede8",
        ],
        "flexoki" | "flexoki-dark" => [
            "#E47DA8", "#CE5D97", "#CE5D97", "#8B7EC8", "#D14D41", "#AF3029", "#DA702C", "#D0A215",
            "#879A39", "#3AA99F", "#5ABDAC", "#66A0C8", "#4385BE", "#A699D0", "#CECDC3", "#B7B5AC",
            "#878580", "#6F6E69", "#575653", "#403E3C", "#343331", "#282726", "#100F0F", "#1C1B1A",
            "#100F0F",
        ],
        "flexoki-light" => [
            "#CE5D97", "#A02F6F", "#A02F6F", "#5E409D", "#AF3029", "#C03E35", "#BC5215", "#AD8301",
            "#66800B", "#24837B", "#2F968D", "#3171B2", "#205EA6", "#735EB5", "#100F0F", "#575653",
            "#6F6E69", "#878580", "#9F9D96", "#B7B5AC", "#CECDC3", "#DAD8CE", "#FFFCF0", "#F2F0E5",
            "#E6E4D9",
        ],
        "monokai" => [
            "#f92672", "#f92672", "#f92672", "#ae81ff", "#f92672", "#f92672", "#fd971f", "#e6db74",
            "#a6e22e", "#66d9ef", "#66d9ef", "#66d9ef", "#66d9ef", "#ae81ff", "#f8f8f2", "#cfcfc2",
            "#75715e", "#75715e", "#49483e", "#49483e", "#3e3d32", "#3e3d32", "#272822", "#23241f",
            "#1e1f1c",
        ],
        _ => return None,
    };
    Some(Palette::from_hexes(&hex))
}

fn catppuccin_mocha() -> Palette {
    builtin("catppuccin-mocha").expect("mocha built-in")
}

fn detect_terminal_mode() -> Option<ThemeMode> {
    use std::io::IsTerminal as _;
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return None;
    }
    let mut opts = QueryOptions::default();
    opts.timeout = std::time::Duration::from_millis(50);
    terminal_colorsaurus::theme_mode(opts).ok()
}

pub fn canonical_skin_name(name: &str) -> &'static str {
    let palette = builtin(&name.trim().to_ascii_lowercase()).unwrap_or_else(catppuccin_mocha);
    BUILTIN_NAMES
        .iter()
        .find(|n| builtin(n) == Some(palette))
        .expect("every built-in palette is listed in BUILTIN_NAMES")
}

pub fn auto_skin_name() -> &'static str {
    match detect_terminal_mode() {
        Some(ThemeMode::Light) => "catppuccin-latte",
        _ => "catppuccin-mocha",
    }
}

pub fn resolve_skin(name: Option<&str>, colors: &HashMap<String, String>) -> Palette {
    let mut p = match name {
        None => builtin(auto_skin_name()).expect("auto skin is a built-in"),
        Some(n) => match builtin(&n.trim().to_ascii_lowercase()) {
            Some(p) => p,
            None => {
                eprintln!(
                    "warning: unknown skin '{n}' (known: {}); using catppuccin-mocha",
                    BUILTIN_NAMES.join(", ")
                );
                catppuccin_mocha()
            }
        },
    };
    for (k, v) in colors {
        let key = k.trim().to_ascii_lowercase();
        match parse_hex(v) {
            Some(c) if p.set(&key, c) => {}
            Some(_) => eprintln!("warning: unknown skin color '{k}' (ignored)"),
            None => eprintln!("warning: invalid hex '{v}' for skin color '{k}' (ignored)"),
        }
    }
    p
}

pub fn validate_skin(name: Option<&str>, colors: &HashMap<String, String>) -> Vec<String> {
    let mut warns = Vec::new();
    if let Some(n) = name
        && builtin(&n.trim().to_ascii_lowercase()).is_none()
    {
        warns.push(format!(
            "skin.name: unknown skin '{n}' (known: {})",
            BUILTIN_NAMES.join(", ")
        ));
    }
    let mut probe = catppuccin_mocha();
    for (k, v) in colors {
        let key = k.trim().to_ascii_lowercase();
        match parse_hex(v) {
            Some(c) if probe.set(&key, c) => {}
            Some(_) => warns.push(format!("skin.colors.{k}: unknown swatch name")),
            None => warns.push(format!(
                "skin.colors.{k}: invalid hex '{v}' (expected #rrggbb)"
            )),
        }
    }
    warns.sort();
    warns
}

fn parse_hex(s: &str) -> Option<Color> {
    let s = s.trim().trim_start_matches('#');
    if s.len() != 6 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let r = u8::from_str_radix(&s[0..2], 16).ok()?;
    let g = u8::from_str_radix(&s[2..4], 16).ok()?;
    let b = u8::from_str_radix(&s[4..6], 16).ok()?;
    Some(Color::Rgb(r, g, b))
}

static ACTIVE: OnceLock<RwLock<Palette>> = OnceLock::new();
static EPOCH: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CACHED: Cell<(u64, Palette)> = Cell::new((0, catppuccin_mocha()));
}

#[cfg(test)]
thread_local! {
    static TEST_PALETTE: Cell<Option<Palette>> = const { Cell::new(None) };
    static TEST_BACKGROUND: Cell<bool> = const { Cell::new(false) };
}

pub fn init(p: Palette) {
    if ACTIVE.set(RwLock::new(p)).is_err() {
        set_shared(p);
    }
    EPOCH.fetch_add(1, Ordering::Release);
}

pub fn set(p: Palette) {
    #[cfg(test)]
    TEST_PALETTE.with(|c| c.set(Some(p)));
    #[cfg(not(test))]
    set_shared(p);
}

fn set_shared(p: Palette) {
    let lock = ACTIVE.get_or_init(|| RwLock::new(catppuccin_mocha()));
    match lock.write() {
        Ok(mut active) => *active = p,
        Err(poisoned) => *poisoned.into_inner() = p,
    }
    EPOCH.fetch_add(1, Ordering::Release);
}

pub fn snapshot() -> Palette {
    palette()
}

fn palette() -> Palette {
    #[cfg(test)]
    return TEST_PALETTE
        .with(|c| c.get())
        .unwrap_or_else(catppuccin_mocha);
    #[cfg(not(test))]
    shared_palette()
}

fn shared_palette() -> Palette {
    let epoch = EPOCH.load(Ordering::Acquire);
    CACHED.with(|c| {
        let (cached_epoch, cached) = c.get();
        if cached_epoch == epoch {
            return cached;
        }
        let fresh = load_active();
        c.set((epoch, fresh));
        fresh
    })
}

fn load_active() -> Palette {
    let lock = ACTIVE.get_or_init(|| RwLock::new(catppuccin_mocha()));
    match lock.read() {
        Ok(active) => *active,
        Err(poisoned) => *poisoned.into_inner(),
    }
}

static BACKGROUND: AtomicBool = AtomicBool::new(false);

pub fn set_background(on: bool) {
    #[cfg(test)]
    TEST_BACKGROUND.with(|c| c.set(on));
    #[cfg(not(test))]
    BACKGROUND.store(on, Ordering::Relaxed);
}

pub fn background() -> Option<Color> {
    #[cfg(test)]
    let on = TEST_BACKGROUND.with(|c| c.get());
    #[cfg(not(test))]
    let on = BACKGROUND.load(Ordering::Relaxed);
    on.then(base)
}

// ---------------------------------------------------------------------------
// Semantic styles
// ---------------------------------------------------------------------------

pub fn title() -> Style {
    Style::default().fg(teal()).add_modifier(Modifier::BOLD)
}

pub fn border() -> Style {
    Style::default().fg(mauve())
}

pub fn border_focused() -> Style {
    Style::default().fg(lavender())
}

pub fn header_row() -> Style {
    Style::default().fg(yellow())
}

pub fn selected_row() -> Style {
    Style::default()
        .bg(lavender())
        .fg(base())
        .add_modifier(Modifier::BOLD)
}

pub fn selected_marked_row() -> Style {
    let bg = mark();
    let fg = if contrast(base(), bg) >= contrast(text(), bg) {
        base()
    } else {
        text()
    };
    Style::default().bg(bg).fg(fg).add_modifier(Modifier::BOLD)
}

fn contrast(a: Color, b: Color) -> f64 {
    fn luminance(color: Color) -> Option<f64> {
        let Color::Rgb(r, g, b) = color else {
            return None;
        };
        let channel = |v: u8| {
            let v = f64::from(v) / 255.0;
            if v <= 0.03928 {
                v / 12.92
            } else {
                ((v + 0.055) / 1.055).powf(2.4)
            }
        };
        Some(0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b))
    }
    match (luminance(a), luminance(b)) {
        (Some(a), Some(b)) => (a.max(b) + 0.05) / (a.min(b) + 0.05),
        _ => 1.0,
    }
}

pub fn sorter() -> Color {
    sky()
}

pub fn counter() -> Color {
    yellow()
}

pub fn mark() -> Color {
    rosewater()
}

pub fn dim() -> Style {
    Style::default().fg(overlay1())
}

pub fn accent() -> Style {
    Style::default().fg(teal())
}

/// Colorize an ns4kafka resource status (`Success`, `Pending`, `Fail`, `Deleting`),
/// Connector `TaskState` (`RUNNING`, `PAUSED`, `FAILED`, `UNASSIGNED`, `DESTROYED`),
/// ConnectCluster status (`HEALTHY`, `IDLE`), or ConsumerGroup `GroupState`
/// (`STABLE`, `EMPTY`, `DEAD`, `PREPARING_REBALANCE`, `COMPLETING_REBALANCE`).
pub fn status_color(s: &str) -> Color {
    with_lowercase(s, |s| match s {
        s if failure_status(s) => red(),
        "running" | "success" | "ready" | "active" | "healthy" | "stable" | "synced" => green(),
        "empty" | "idle" | "completed" | "stopped" => overlay0(),
        s if pending_status(s) => yellow(),
        "deleting" | "terminating" => mauve(),
        "unknown" | "" => overlay1(),
        _ => text(),
    })
}

fn with_lowercase<R>(s: &str, f: impl FnOnce(&str) -> R) -> R {
    let mut buf = [0u8; 48];
    match buf.get_mut(..s.len()) {
        Some(lower) => {
            lower.copy_from_slice(s.as_bytes());
            lower.make_ascii_lowercase();
            f(std::str::from_utf8(lower).expect("ASCII lowercasing keeps UTF-8 valid"))
        }
        None => f(&s.to_ascii_lowercase()),
    }
}

fn pending_status(status: &str) -> bool {
    matches!(
        status,
        "pending"
            | "paused"
            | "unassigned"
            | "preparing_rebalance"
            | "completing_rebalance"
            | "progressing"
            | "reconciling"
            | "suspended"
    )
}

fn failure_status(status: &str) -> bool {
    matches!(
        status,
        "fail"
            | "failed"
            | "error"
            | "dead"
            | "destroyed"
            | "unhealthy"
            | "degraded"
            | "unavailable"
    )
}

pub fn row_color(s: &str) -> Color {
    with_lowercase(s, |s| match s {
        s if failure_status(s) => red(),
        s if pending_status(s) => peach(),
        "empty" | "idle" | "completed" | "stopped" => overlay0(),
        "deleting" | "terminating" => mauve(),
        _ => blue(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_builtin_skins_resolve() {
        for name in BUILTIN_NAMES {
            assert!(builtin(name).is_some(), "missing skin {name}");
        }
    }

    #[test]
    fn ns4kafka_statuses_map_to_expected_colors() {
        assert_eq!(status_color("Success"), green());
        assert_eq!(status_color("RUNNING"), green());
        assert_eq!(status_color("Pending"), yellow());
        assert_eq!(status_color("PAUSED"), yellow());
        assert_eq!(status_color("UNASSIGNED"), yellow());
        assert_eq!(status_color("Fail"), red());
        assert_eq!(status_color("FAILED"), red());
        assert_eq!(row_color("Fail"), red());
        assert_eq!(row_color("Pending"), peach());
        assert_eq!(row_color("Success"), blue());
    }
}
