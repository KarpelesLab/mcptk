//! Matching URIs against RFC 6570 URI templates, as used by resource
//! templates.
//!
//! Supports simple expansion (`{var}`, matching one path segment) and
//! reserved expansion (`{+var}`, matching anything, slashes included). Other
//! operators are matched like `{+var}`; values are percent-decoded.

use std::collections::HashMap;

#[derive(Debug)]
enum Part<'a> {
    Literal(&'a str),
    Var { name: &'a str, reserved: bool },
}

fn parse(template: &str) -> Option<Vec<Part<'_>>> {
    let mut parts = Vec::new();
    let mut rest = template;
    while !rest.is_empty() {
        match rest.find('{') {
            Some(0) => {
                let end = rest.find('}')?;
                let expr = &rest[1..end];
                let (reserved, name) = match expr.chars().next()? {
                    c if c.is_ascii_alphanumeric() || c == '_' => (false, expr),
                    _ => (true, &expr[1..]),
                };
                // Explode/prefix modifiers aren't supported: drop them.
                let name = name.split([':', '*', ',']).next().unwrap_or(name);
                parts.push(Part::Var { name, reserved });
                rest = &rest[end + 1..];
            }
            Some(i) => {
                parts.push(Part::Literal(&rest[..i]));
                rest = &rest[i..];
            }
            None => {
                parts.push(Part::Literal(rest));
                rest = "";
            }
        }
    }
    Some(parts)
}

/// Match `uri` against `template`, returning the variables on success.
pub fn match_uri_template(template: &str, uri: &str) -> Option<HashMap<String, String>> {
    let parts = parse(template)?;
    let mut vars = HashMap::new();
    match_parts(&parts, uri, &mut vars).then_some(vars)
}

fn match_parts(parts: &[Part<'_>], uri: &str, vars: &mut HashMap<String, String>) -> bool {
    match parts.split_first() {
        None => uri.is_empty(),
        Some((Part::Literal(lit), rest)) => uri.strip_prefix(lit).is_some_and(|u| match_parts(rest, u, vars)),
        Some((Part::Var { name, reserved }, rest)) => {
            // A simple variable stops at the first '/', '?' or '#'.
            let limit = if *reserved { uri.len() } else { uri.find(['/', '?', '#']).unwrap_or(uri.len()) };
            // Prefer the longest value, so a trailing variable takes the rest.
            for end in (0..=limit).rev() {
                if !uri.is_char_boundary(end) {
                    continue;
                }
                if match_parts(rest, &uri[end..], vars) {
                    if end == 0 && !*reserved {
                        return false; // a simple variable can't be empty
                    }
                    vars.insert(name.to_string(), percent_decode(&uri[..end]));
                    return true;
                }
            }
            false
        }
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(t: &str, u: &str) -> Option<Vec<(String, String)>> {
        match_uri_template(t, u).map(|v| {
            let mut v: Vec<_> = v.into_iter().collect();
            v.sort();
            v
        })
    }

    #[test]
    fn matches() {
        assert_eq!(m("users://{id}/profile", "users://42/profile"), Some(vec![("id".into(), "42".into())]));
        assert_eq!(m("users://{id}/profile", "users://42/x/profile"), None);
        assert_eq!(m("file:///{+path}", "file:///a/b/c.txt"), Some(vec![("path".into(), "a/b/c.txt".into())]));
        assert_eq!(
            m("db://{table}/{row}", "db://t/r%20x"),
            Some(vec![("row".into(), "r x".into()), ("table".into(), "t".into())])
        );
        assert_eq!(m("db://{table}", "db://"), None);
        assert_eq!(m("static://x", "static://x"), Some(vec![]));
        assert_eq!(m("static://x", "static://xy"), None);
    }
}
