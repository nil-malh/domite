// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Key chords for user-bindable actions.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyChord {
    pub code: KeyCode,
    pub ctrl: bool,
    pub alt: bool,
    pub shift: bool,
}

impl KeyChord {
    pub fn parse(s: &str) -> Result<KeyChord, String> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err("empty key binding".into());
        }
        if trimmed.chars().count() == 1 {
            let c = trimmed.chars().next().unwrap();
            return Ok(KeyChord {
                code: KeyCode::Char(c),
                ctrl: false,
                alt: false,
                shift: false,
            });
        }

        let (mods, key) = if let Some(mods) = trimmed.strip_suffix("--") {
            (Some(mods), "-")
        } else if let Some((mods, key)) = trimmed.rsplit_once('-') {
            (Some(mods), key)
        } else {
            (None, trimmed)
        };

        let (mut ctrl, mut alt, mut shift) = (false, false, false);
        for m in mods.into_iter().flat_map(|mods| mods.split('-')) {
            match m.to_ascii_lowercase().as_str() {
                "ctrl" | "control" | "c" => ctrl = true,
                "alt" | "option" | "opt" | "meta" | "a" | "m" => alt = true,
                "shift" | "s" => shift = true,
                "" => return Err(format!("{s:?}: empty modifier (stray '-'?)")),
                other => return Err(format!("{s:?}: unknown modifier {other:?}")),
            }
        }

        let mut code = parse_key(key).ok_or_else(|| format!("{s:?}: unknown key {key:?}"))?;
        if shift && code == KeyCode::Char('-') {
            return Err(format!(
                "{s:?}: Shift with minus is not supported; bind the resulting character instead (for example, \"_\")"
            ));
        }

        if let (true, KeyCode::Char(c)) = (shift, code)
            && c.is_ascii_alphabetic()
        {
            code = KeyCode::Char(c.to_ascii_uppercase());
            shift = false;
        }

        if let (true, KeyCode::Tab) = (shift, code) {
            code = KeyCode::BackTab;
            shift = false;
        }

        Ok(KeyChord {
            code,
            ctrl,
            alt,
            shift,
        })
    }

    pub fn matches(&self, event: &KeyEvent) -> bool {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let alt = event.modifiers.contains(KeyModifiers::ALT);
        if ctrl != self.ctrl || alt != self.alt {
            return false;
        }
        match (self.code, event.code) {
            (KeyCode::Char(a), KeyCode::Char(b)) => {
                if self.ctrl {
                    a.eq_ignore_ascii_case(&b)
                } else {
                    a == b
                }
            }
            (KeyCode::BackTab, KeyCode::BackTab) => true,
            (a, b) => a == b && self.shift == event.modifiers.contains(KeyModifiers::SHIFT),
        }
    }

    pub fn label(&self) -> String {
        let mut out = String::new();
        if self.ctrl {
            out.push_str("ctrl-");
        }
        if self.alt {
            out.push_str("alt-");
        }
        if self.shift {
            out.push_str("shift-");
        }
        out.push_str(&key_label(self.code));
        out
    }
}

fn parse_key(token: &str) -> Option<KeyCode> {
    if token.chars().count() == 1 {
        return Some(KeyCode::Char(token.chars().next().unwrap()));
    }
    let lower = token.to_ascii_lowercase();
    if let Some(n) = lower.strip_prefix('f')
        && let Ok(n) = n.parse::<u8>()
        && (1..=24).contains(&n)
    {
        return Some(KeyCode::F(n));
    }
    Some(match lower.as_str() {
        "enter" | "return" | "ret" => KeyCode::Enter,
        "tab" => KeyCode::Tab,
        "backtab" => KeyCode::BackTab,
        "esc" | "escape" => KeyCode::Esc,
        "space" => KeyCode::Char(' '),
        "backspace" | "bs" => KeyCode::Backspace,
        "delete" | "del" => KeyCode::Delete,
        "insert" | "ins" => KeyCode::Insert,
        "home" => KeyCode::Home,
        "end" => KeyCode::End,
        "pageup" | "pgup" => KeyCode::PageUp,
        "pagedown" | "pgdn" | "pgdown" => KeyCode::PageDown,
        "up" => KeyCode::Up,
        "down" => KeyCode::Down,
        "left" => KeyCode::Left,
        "right" => KeyCode::Right,
        _ => return None,
    })
}

fn key_label(code: KeyCode) -> String {
    match code {
        KeyCode::Char(' ') => "space".into(),
        KeyCode::Char(c) => c.to_string(),
        KeyCode::F(n) => format!("f{n}"),
        KeyCode::Enter => "enter".into(),
        KeyCode::Tab => "tab".into(),
        KeyCode::BackTab => "backtab".into(),
        KeyCode::Esc => "esc".into(),
        KeyCode::Backspace => "backspace".into(),
        KeyCode::Delete => "delete".into(),
        KeyCode::Insert => "insert".into(),
        KeyCode::Home => "home".into(),
        KeyCode::End => "end".into(),
        KeyCode::PageUp => "pageup".into(),
        KeyCode::PageDown => "pagedown".into(),
        KeyCode::Up => "up".into(),
        KeyCode::Down => "down".into(),
        KeyCode::Left => "left".into(),
        KeyCode::Right => "right".into(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_and_modified_chords() {
        let c = KeyChord::parse("ctrl-d").unwrap();
        assert_eq!(c.code, KeyCode::Char('d'));
        assert!(c.ctrl && !c.alt);
        assert_eq!(c.label(), "ctrl-d");

        let bt = KeyChord::parse("shift-tab").unwrap();
        assert_eq!(bt.code, KeyCode::BackTab);
        assert!(!bt.shift);

        let fkey = KeyChord::parse("alt-shift-f5").unwrap();
        assert_eq!(fkey.code, KeyCode::F(5));
        assert!(fkey.alt && fkey.shift && !fkey.ctrl);
        assert_eq!(fkey.label(), "alt-shift-f5");

        let shifted_letter = KeyChord::parse("shift-s").unwrap();
        assert_eq!(shifted_letter.code, KeyCode::Char('S'));
        assert!(!shifted_letter.shift);
        assert!(shifted_letter.matches(&KeyEvent::new(KeyCode::Char('S'), KeyModifiers::SHIFT)));
        assert!(!shifted_letter.matches(&KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE)));

        // Reject invalid chords
        assert!(KeyChord::parse("").is_err());
        assert!(KeyChord::parse("unknownmod-a").is_err());
        assert!(KeyChord::parse("ctrl-unknownkey").is_err());
        assert!(KeyChord::parse("shift--").is_err());
    }
}
