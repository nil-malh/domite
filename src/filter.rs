// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Structured row-filter grammar (`/` filter language).
#![allow(dead_code)]
//!
//! Supports:
//! - `text`                   contiguous, case-insensitive text match
//! - `a|b`                    either text
//! - `"text"`                 exact text match with spaces
//! - `~text`                  fuzzy subsequence match
//! - `/re/`                   regular expression (case-insensitive)
//! - `!text`                  inverse match (`!"text"`, `!/re/`, `!~fuzzy`)
//! - `label:text`             match against resource `metadata.labels`
//! - `status=Fail`            column equality (case-insensitive)
//! - `partitions>=6` `lag>0`  numeric comparisons
//! - `age<2h`                 duration comparisons (`90s`, `2h`, `1d2h`)
//! - `&&`, `||`, `(group)`    boolean combinations

use std::collections::HashMap;

use crate::fuzzy::Fuzzy;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Structured {
    pub terms: Vec<Term>,
    pub labels: Option<String>,
    pub fields: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Text { negate: bool, pat: Pattern },
    Label { negate: bool, pat: Pattern },
    Cmp(Cmp),
    All(Vec<Term>),
    Any(Vec<Term>),
    Not(Vec<Term>),
}

impl Term {
    pub fn time_sensitive(&self) -> bool {
        match self {
            Self::Cmp(Cmp {
                value: CmpValue::Duration(_),
                ..
            }) => true,
            Self::All(terms) | Self::Any(terms) | Self::Not(terms) => {
                terms.iter().any(Self::time_sensitive)
            }
            _ => false,
        }
    }

    fn highlight_pattern(&self) -> Option<&Pattern> {
        match self {
            Self::Text { negate: false, pat } => Some(pat),
            Self::All(terms) | Self::Any(terms) => terms.iter().find_map(Self::highlight_pattern),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub enum Pattern {
    Fuzzy(String),
    Literal(Literal),
    Regex(Box<regex::Regex>),
}

impl Pattern {
    pub fn text(&self) -> &str {
        match self {
            Pattern::Fuzzy(pat) => pat,
            Pattern::Literal(lit) => lit.text(),
            Pattern::Regex(re) => re.as_str(),
        }
    }

    pub fn matches(&self, haystack: &str, fuzzy: &Fuzzy) -> bool {
        match self {
            Pattern::Fuzzy(needle) => fuzzy.score(haystack, needle).is_some(),
            Pattern::Literal(lit) => lit.matches(haystack),
            Pattern::Regex(re) => re.is_match(haystack),
        }
    }
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Pattern::Fuzzy(a), Pattern::Fuzzy(b)) => a == b,
            (Pattern::Literal(a), Pattern::Literal(b)) => a.text() == b.text(),
            (Pattern::Regex(a), Pattern::Regex(b)) => a.as_str() == b.as_str(),
            _ => false,
        }
    }
}

impl std::fmt::Debug for Pattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pattern::Fuzzy(pat) => write!(f, "Fuzzy({pat:?})"),
            Pattern::Literal(lit) => write!(f, "Literal({:?})", lit.text()),
            Pattern::Regex(re) => write!(f, "Regex({:?})", re.as_str()),
        }
    }
}

#[derive(Clone)]
pub struct Literal {
    text: String,
    lower: String,
}

impl Literal {
    fn new(text: &str) -> Self {
        Literal {
            text: text.to_string(),
            lower: fold_lower(text),
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn matches(&self, haystack: &str) -> bool {
        if haystack.is_ascii() && self.lower.is_ascii() {
            let needle = self.lower.as_bytes();
            if needle.is_empty() {
                return true;
            }
            if needle.len() > haystack.len() {
                return false;
            }
            haystack
                .as_bytes()
                .windows(needle.len())
                .any(|w| w.eq_ignore_ascii_case(needle))
        } else {
            haystack.to_lowercase().contains(&self.lower)
        }
    }

    pub fn match_span(&self, haystack: &str) -> Option<std::ops::Range<usize>> {
        let fold = |c: char| c.to_lowercase().next().unwrap_or(c);
        let hay: Vec<char> = haystack.chars().map(fold).collect();
        let needle: Vec<char> = self.text.chars().map(fold).collect();
        if needle.is_empty() || needle.len() > hay.len() {
            return None;
        }
        (0..=hay.len() - needle.len())
            .find(|&i| hay[i..i + needle.len()] == needle[..])
            .map(|i| i..i + needle.len())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cmp {
    pub key: String,
    pub op: Op,
    pub value: CmpValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl Op {
    pub fn eval(self, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            Op::Eq => ord == Equal,
            Op::Ne => ord != Equal,
            Op::Gt => ord == Greater,
            Op::Ge => ord != Less,
            Op::Lt => ord == Less,
            Op::Le => ord != Greater,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CmpValue {
    Num(f64),
    Duration(i64),
    Str(String),
}

impl Structured {
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn highlight_pattern(&self) -> Option<&Pattern> {
        self.terms.iter().find_map(Term::highlight_pattern)
    }

    /// Evaluate this filter against a row's rendered cells, labels, and optional age in seconds.
    #[allow(clippy::too_many_arguments)]
    pub fn matches_row(
        &self,
        namespace: Option<&str>,
        name: &str,
        headers: &[String],
        cells: &[String],
        labels: &HashMap<String, String>,
        age_secs: Option<i64>,
        fuzzy: &Fuzzy,
    ) -> bool {
        eval_terms(
            &self.terms,
            namespace,
            name,
            headers,
            cells,
            labels,
            age_secs,
            fuzzy,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_terms(
    terms: &[Term],
    namespace: Option<&str>,
    name: &str,
    headers: &[String],
    cells: &[String],
    labels: &HashMap<String, String>,
    age_secs: Option<i64>,
    fuzzy: &Fuzzy,
) -> bool {
    terms.iter().all(|term| {
        eval_term(
            term, namespace, name, headers, cells, labels, age_secs, fuzzy,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn eval_term(
    term: &Term,
    namespace: Option<&str>,
    name: &str,
    headers: &[String],
    cells: &[String],
    labels: &HashMap<String, String>,
    age_secs: Option<i64>,
    fuzzy: &Fuzzy,
) -> bool {
    match term {
        Term::Text { negate, pat } => {
            let ns_name = match namespace {
                Some(ns) if !ns.is_empty() => format!("{ns} {name}"),
                _ => name.to_string(),
            };
            let hit =
                pat.matches(&ns_name, fuzzy) || cells.iter().any(|cell| pat.matches(cell, fuzzy));
            if *negate { !hit } else { hit }
        }
        Term::Label { negate, pat } => {
            let hit = labels.iter().any(|(k, v)| {
                let kv = format!("{k}={v}");
                pat.matches(k, fuzzy) || pat.matches(v, fuzzy) || pat.matches(&kv, fuzzy)
            });
            if *negate { !hit } else { hit }
        }
        Term::Cmp(cmp) => eval_cmp(cmp, headers, cells, age_secs),
        Term::All(sub) => eval_terms(
            sub, namespace, name, headers, cells, labels, age_secs, fuzzy,
        ),
        Term::Any(sub) => sub
            .iter()
            .any(|t| eval_term(t, namespace, name, headers, cells, labels, age_secs, fuzzy)),
        Term::Not(sub) => !eval_terms(
            sub, namespace, name, headers, cells, labels, age_secs, fuzzy,
        ),
    }
}

fn eval_cmp(cmp: &Cmp, headers: &[String], cells: &[String], age_secs: Option<i64>) -> bool {
    if cmp.key == "age"
        && let CmpValue::Duration(want) = cmp.value
    {
        let Some(actual) = age_secs else {
            return false;
        };
        return cmp.op.eval(actual.cmp(&want));
    }

    let col_idx = headers.iter().position(|h| {
        let h_lower = h.to_ascii_lowercase();
        h_lower == cmp.key
            || h_lower.replace('-', "_") == cmp.key.replace('-', "_")
            || (cmp.key == "ns" && h_lower == "namespace")
            || (cmp.key == "rf" && h_lower == "replication-factor")
    });
    let Some(idx) = col_idx else {
        return false;
    };
    let Some(cell) = cells.get(idx) else {
        return false;
    };

    match &cmp.value {
        CmpValue::Num(want) => {
            let Some(actual) = cell_number(cell) else {
                return false;
            };
            let Some(ord) = actual.partial_cmp(want) else {
                return false;
            };
            cmp.op.eval(ord)
        }
        CmpValue::Duration(want) => {
            let Some(actual) = parse_duration(cell) else {
                return false;
            };
            cmp.op.eval(actual.cmp(want))
        }
        CmpValue::Str(want) => cmp.op.eval(cmp_folded_lower(cell.trim(), want)),
    }
}

pub fn parse(input: &str) -> Structured {
    let trimmed = input.trim();
    if !is_structured(trimmed) {
        return plain(trimmed);
    }

    match tokenize(trimmed) {
        Ok(tokens) => parse_tokens(&tokens, 0),
        Err(error) => Structured {
            error: Some(error),
            ..Structured::default()
        },
    }
}

fn plain(input: &str) -> Structured {
    let mut s = Structured::default();
    for word in input.split_whitespace() {
        match pattern(word) {
            Ok(pat) => s.terms.push(Term::Text { negate: false, pat }),
            Err(e) => {
                s.error.get_or_insert(e);
            }
        }
    }
    s
}

fn parse_tokens(tokens: &[String], depth: usize) -> Structured {
    if depth > 32 {
        return Structured {
            error: Some("filter nesting exceeds 32 levels".into()),
            ..Structured::default()
        };
    }
    if tokens.iter().any(|t| t == "||") {
        let mut branches = Vec::new();
        for branch in tokens.split(|t| t == "||") {
            let parsed = parse_tokens(branch, depth + 1);
            let error = if branch.is_empty() {
                Some("expected terms on both sides of ||".into())
            } else {
                parsed.error
            };
            if let Some(error) = error {
                return Structured {
                    error: Some(error),
                    ..Structured::default()
                };
            }
            branches.push(Term::All(parsed.terms));
        }
        return Structured {
            terms: vec![Term::Any(branches)],
            ..Structured::default()
        };
    }
    let mut terms = Vec::new();
    let mut error: Option<String> = None;
    let fail = |slot: &mut Option<String>, msg: String| {
        if slot.is_none() {
            *slot = Some(msg);
        }
    };

    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i].as_str();
        i += 1;
        let group = tok
            .strip_prefix("!(")
            .map(|s| (s, true))
            .or_else(|| tok.strip_prefix('(').map(|s| (s, false)));
        if let Some((inner, inverse)) = group {
            let parsed = inner
                .strip_suffix(')')
                .ok_or("unclosed group".to_string())
                .and_then(tokenize)
                .map(|tokens| parse_tokens(&tokens, depth + 1));
            match parsed {
                Ok(s) if s.error.is_some() => fail(&mut error, s.error.unwrap()),
                Ok(s) if s.terms.is_empty() => fail(&mut error, "empty Boolean group".into()),
                Ok(s) => terms.push(if inverse {
                    Term::Not(s.terms)
                } else {
                    Term::All(s.terms)
                }),
                Err(e) => fail(&mut error, e),
            }
            continue;
        }
        if tok == "&&" {
            if i == 1 || i == tokens.len() || tokens[i] == "&&" {
                fail(&mut error, "expected terms on both sides of &&".into());
            }
            continue;
        }
        if let Some((key, op, value)) = split_cmp(tok) {
            if value.is_empty() {
                fail(&mut error, format!("missing value in '{tok}'"));
                continue;
            }
            match typed_value(key, unquote(value)) {
                Ok(v) => terms.push(Term::Cmp(Cmp {
                    key: key.to_ascii_lowercase(),
                    op,
                    value: v,
                })),
                Err(e) => fail(&mut error, e),
            }
            continue;
        }
        let (negate, rest) = match tok.strip_prefix('!') {
            Some(rest) => (true, rest),
            None => (false, tok),
        };
        let label = rest.strip_prefix("label:");
        let rest = label.unwrap_or(rest);
        if rest.is_empty() {
            fail(
                &mut error,
                if label.is_some() {
                    "expected pattern after 'label:'"
                } else {
                    "expected text after '!'"
                }
                .into(),
            );
            continue;
        }
        match pattern(rest) {
            Ok(Pattern::Fuzzy(_)) if label.is_some() => fail(
                &mut error,
                "label patterns cannot be fuzzy; drop the '~'".into(),
            ),
            Ok(pat) if label.is_some() => terms.push(Term::Label { negate, pat }),
            Ok(pat) => terms.push(Term::Text { negate, pat }),
            Err(e) => fail(&mut error, e),
        }
    }

    Structured {
        terms,
        labels: None,
        fields: None,
        error,
    }
}

fn is_structured(input: &str) -> bool {
    let structured = |tok: &str| {
        let text = tok.strip_prefix('!').unwrap_or(tok);
        tok == "&&"
            || tok == "||"
            || tok.starts_with('!')
            || tok.starts_with('(')
            || text.starts_with('"')
            || text.starts_with("label:")
            || is_regex(text)
            || split_cmp(tok).is_some()
    };
    match tokenize(input) {
        Ok(tokens) => tokens.iter().any(|t| structured(t)),
        Err(_) => input.split_whitespace().any(structured),
    }
}

fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quote = None;
    let mut depth = 0usize;
    let mut chars = input.char_indices().peekable();
    while let Some((offset, c)) = chars.next() {
        if let Some(q) = quote {
            token.push(c);
            if c == q {
                quote = None;
            }
        } else if c == '/'
            && at_pattern_start(&input[..offset])
            && let Some(end) = regex_end(&input[offset..], 1)
        {
            token.push_str(&input[offset..offset + end]);
            while chars.peek().is_some_and(|(i, _)| *i < offset + end) {
                chars.next();
            }
        } else if c == '\'' || c == '"' {
            quote = Some(c);
            token.push(c);
        } else if c == '(' {
            depth += 1;
            token.push(c);
        } else if c == ')' {
            depth = depth.checked_sub(1).ok_or("unexpected ')' in filter")?;
            token.push(c);
        } else if depth == 0
            && matches!(c, '&' | '|')
            && chars.peek().is_some_and(|(_, next)| *next == c)
        {
            if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
            chars.next();
            tokens.push(format!("{c}{c}"));
        } else if c.is_whitespace() && depth == 0 {
            if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
        } else {
            token.push(c);
        }
    }
    let text = token.strip_prefix('!').unwrap_or(&token);
    let text = text.strip_prefix("label:").unwrap_or(text);
    if depth != 0 || quote.is_some() && !text.starts_with('"') {
        return Err("unclosed quote or group".into());
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    Ok(tokens)
}

fn at_pattern_start(input: &str) -> bool {
    input
        .strip_suffix("label:")
        .unwrap_or(input)
        .trim_end_matches('!')
        .chars()
        .next_back()
        .is_none_or(|previous| previous.is_whitespace() || matches!(previous, '(' | '&' | '|'))
}

fn unquote(value: &str) -> &str {
    for quote in ['\'', '"'] {
        if let Some(inner) = value
            .strip_prefix(quote)
            .and_then(|s| s.strip_suffix(quote))
        {
            return inner;
        }
    }
    value
}

fn regex_end(input: &str, start: usize) -> Option<usize> {
    let mut escaped = false;
    let mut classes = 0usize;
    let mut fallback = None;
    for (offset, c) in input[start..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '[' => classes += 1,
            ']' => classes = classes.saturating_sub(1),
            '/' => {
                let end = start + offset + 1;
                if input[end..]
                    .chars()
                    .next()
                    .is_none_or(|c| c.is_whitespace() || matches!(c, ')' | '&' | '|'))
                {
                    if classes == 0 {
                        return Some(end);
                    }
                    fallback.get_or_insert(end);
                }
            }
            _ => {}
        }
    }
    fallback
}

fn pattern(tok: &str) -> Result<Pattern, String> {
    if let Some(rest) = tok.strip_prefix('"') {
        let text = rest.strip_suffix('"').unwrap_or(rest);
        if text.is_empty() {
            return Err("expected text between the quotes".into());
        }
        return Ok(Pattern::Literal(Literal::new(text)));
    }
    if is_regex(tok) {
        let source = &tok[1..tok.len() - 1];
        if source.is_empty() {
            return Err("expected a pattern between the slashes".into());
        }
        return regex::RegexBuilder::new(source)
            .case_insensitive(true)
            .build()
            .map(|re| Pattern::Regex(Box::new(re)))
            .map_err(|_| format!("bad regex '{source}'"));
    }
    if let Some(rest) = tok.strip_prefix('~') {
        if rest.is_empty() {
            return Err("expected text after '~'".into());
        }
        return Ok(Pattern::Fuzzy(rest.to_string()));
    }
    if tok.contains('|') {
        let alternatives: Vec<&str> = tok.split('|').filter(|s| !s.is_empty()).collect();
        return match alternatives[..] {
            [] => Err("expected text around '|'".into()),
            [text] => Ok(Pattern::Literal(Literal::new(text))),
            _ => {
                let source = alternatives
                    .iter()
                    .map(|s| regex::escape(s))
                    .collect::<Vec<_>>()
                    .join("|");
                regex::RegexBuilder::new(&source)
                    .case_insensitive(true)
                    .build()
                    .map(|re| Pattern::Regex(Box::new(re)))
                    .map_err(|_| format!("bad alternatives '{tok}'"))
            }
        };
    }
    Ok(Pattern::Literal(Literal::new(tok)))
}

fn is_regex(tok: &str) -> bool {
    tok.len() >= 2 && tok.starts_with('/') && tok.ends_with('/')
}

fn split_cmp(tok: &str) -> Option<(&str, Op, &str)> {
    if !tok
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '%'))
    {
        return None;
    }
    let key_end = tok
        .find(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '%' | '/')))?;
    let (key, rest) = tok.split_at(key_end);
    let (op, value) = if let Some(v) = rest.strip_prefix("!=") {
        (Op::Ne, v)
    } else if let Some(v) = rest.strip_prefix(">=") {
        (Op::Ge, v)
    } else if let Some(v) = rest.strip_prefix("<=") {
        (Op::Le, v)
    } else if let Some(v) = rest.strip_prefix("==") {
        (Op::Eq, v)
    } else if let Some(v) = rest.strip_prefix('=') {
        (Op::Eq, v)
    } else if let Some(v) = rest.strip_prefix('>') {
        (Op::Gt, v)
    } else {
        (Op::Lt, rest.strip_prefix('<')?)
    };
    Some((key, op, value))
}

fn fold_lower(s: &str) -> String {
    if s.is_ascii() {
        s.to_ascii_lowercase()
    } else {
        s.to_lowercase()
    }
}

pub fn cmp_folded_lower(cell: &str, want: &str) -> std::cmp::Ordering {
    if cell.is_ascii() && want.is_ascii() {
        cell.bytes()
            .map(|byte| byte.to_ascii_lowercase())
            .cmp(want.bytes())
    } else {
        cell.to_lowercase().as_str().cmp(want)
    }
}

fn typed_value(key: &str, raw: &str) -> Result<CmpValue, String> {
    match key.to_ascii_lowercase().as_str() {
        "age" => parse_duration(raw)
            .map(CmpValue::Duration)
            .ok_or_else(|| format!("bad duration '{raw}'")),
        _ => match raw.parse::<f64>() {
            Ok(value) if value.is_finite() => Ok(CmpValue::Num(value)),
            Ok(_) => Err(format!("non-finite number '{raw}'")),
            Err(_) => Ok(CmpValue::Str(fold_lower(raw))),
        },
    }
}

pub fn cell_number(cell: &str) -> Option<f64> {
    let number = cell
        .trim()
        .split([' ', '/', '('])
        .next()?
        .parse::<f64>()
        .ok()?;
    number.is_finite().then_some(number)
}

pub fn parse_duration(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(v) = s.parse::<i64>() {
        return (v >= 0).then_some(v);
    }
    let mut total = 0i64;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let unit = match c {
            's' => 1,
            'm' => 60,
            'h' => 3_600,
            'd' => 86_400,
            'w' => 604_800,
            _ => return None,
        };
        if num.is_empty() {
            return None;
        }
        total = total.checked_add(num.parse::<i64>().ok()?.checked_mul(unit)?)?;
        num.clear();
    }
    num.is_empty().then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_evaluates_structured_filters() {
        let f = Fuzzy::new();
        let headers = vec![
            "NAME".to_string(),
            "PARTITIONS".to_string(),
            "STATUS".to_string(),
            "AGE".to_string(),
        ];
        let cells = vec![
            "orders.payments.v1".to_string(),
            "6".to_string(),
            "Success".to_string(),
            "1h".to_string(),
        ];
        let labels = HashMap::new();

        let s = parse("orders partitions>=6 status=Success age<2h");
        assert!(s.error().is_none());
        assert!(s.matches_row(
            Some("team-a"),
            "orders.payments.v1",
            &headers,
            &cells,
            &labels,
            Some(3600),
            &f
        ));

        let s_fail = parse("status=Fail || partitions>12");
        assert!(!s_fail.matches_row(
            Some("team-a"),
            "orders.payments.v1",
            &headers,
            &cells,
            &labels,
            Some(3600),
            &f
        ));

        let s_fuzzy = parse("~opv !legacy");
        assert!(s_fuzzy.matches_row(
            Some("team-a"),
            "orders.payments.v1",
            &headers,
            &cells,
            &labels,
            Some(3600),
            &f
        ));
    }
}
