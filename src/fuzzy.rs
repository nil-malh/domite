// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The fuzzy matcher behind row filtering, the pickers and `find`.
#![allow(dead_code)]

use std::cell::RefCell;

use nucleo_matcher::pattern::{Atom, AtomKind, CaseMatching, Normalization};
use nucleo_matcher::{Config, Matcher, Utf32Str};
use unicode_segmentation::UnicodeSegmentation;

pub struct Fuzzy {
    inner: RefCell<Inner>,
}

struct Inner {
    matcher: Matcher,
    needle: String,
    atom: Atom,
    haystack: Vec<char>,
    indices: Vec<u32>,
}

impl Inner {
    fn sync(&mut self, needle: &str) {
        if self.needle != needle {
            self.needle.clear();
            self.needle.push_str(needle);
            self.atom = atom(needle);
        }
    }
}

fn atom(needle: &str) -> Atom {
    Atom::new(
        needle,
        CaseMatching::Smart,
        Normalization::Smart,
        AtomKind::Fuzzy,
        false,
    )
}

fn utf32<'a>(text: &'a str, buf: &'a mut Vec<char>) -> Utf32Str<'a> {
    if text.is_ascii() {
        return Utf32Str::Ascii(text.as_bytes());
    }
    buf.clear();
    buf.extend(nucleo_matcher::chars::graphemes(text));
    Utf32Str::Unicode(&*buf)
}

impl Fuzzy {
    pub fn new() -> Self {
        Fuzzy {
            inner: RefCell::new(Inner {
                matcher: Matcher::new(Config::DEFAULT),
                needle: String::new(),
                atom: atom(""),
                haystack: Vec::new(),
                indices: Vec::new(),
            }),
        }
    }

    pub fn score(&self, haystack: &str, needle: &str) -> Option<i64> {
        let mut inner = self.inner.borrow_mut();
        inner.sync(needle);
        let Inner {
            matcher,
            atom,
            haystack: buf,
            ..
        } = &mut *inner;
        atom.score(utf32(haystack, buf), matcher).map(i64::from)
    }

    pub fn indices(&self, haystack: &str, needle: &str) -> Option<Vec<usize>> {
        let mut inner = self.inner.borrow_mut();
        inner.sync(needle);
        let Inner {
            matcher,
            atom,
            haystack: buf,
            indices,
            ..
        } = &mut *inner;
        indices.clear();
        atom.indices(utf32(haystack, buf), matcher, indices)?;
        indices.sort_unstable();
        indices.dedup();
        if haystack.is_ascii() {
            return Some(indices.iter().map(|&i| i as usize).collect());
        }

        let mut matched = indices.iter().peekable();
        let mut positions = Vec::with_capacity(indices.len());
        let mut char_offset = 0;
        for (i, grapheme) in haystack.graphemes(true).enumerate() {
            if matched.peek().is_none() {
                break;
            }
            let end = char_offset + grapheme.chars().count();
            if matched.peek().is_some_and(|&&pos| pos as usize == i) {
                positions.extend(char_offset..end);
                matched.next();
            }
            char_offset = end;
        }
        Some(positions)
    }
}

impl Default for Fuzzy {
    fn default() -> Self {
        Fuzzy::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_subsequences_and_reports_their_positions() {
        let f = Fuzzy::new();
        assert!(f.score("orders.payments.v1", "opv").is_some());
        assert!(f.score("orders.payments.v1", "zzz").is_none());

        let idx = f.indices("orders.payments.v1", "opv").unwrap();
        assert_eq!(idx.len(), 3);
        assert!(idx.is_sorted());
    }

    #[test]
    fn case_is_smart() {
        let f = Fuzzy::new();
        assert!(f.score("Orders-Topic", "orders").is_some());
        assert!(f.score("Orders-Topic", "Orders").is_some());
        assert!(f.score("orders-topic", "Orders").is_none());
    }
}
