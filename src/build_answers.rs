use crate::answers::{parse_tx_id, write_haproxy_map, write_index};
use crate::parser::parse_payload;
use std::fs;

pub fn run(input: &str, output: &str, map_output: Option<&str>) -> Result<(), String> {
    let data = fs::read_to_string(input).map_err(|e| format!("failed to read {input}: {e}"))?;
    let mut cursor = 0usize;
    let mut rows = Vec::new();

    while let Some(request_key_pos) = data[cursor..].find("\"request\"") {
        let request_key_pos = cursor + request_key_pos;
        let Some((request_start, request_end)) = object_after_key(&data, request_key_pos) else {
            return Err("bad request object in test data".to_string());
        };
        let expected_pos = data[request_end..]
            .find("\"expected_approved\"")
            .map(|pos| request_end + pos)
            .ok_or_else(|| "missing expected_approved".to_string())?;
        let expected = bool_after_key(&data, expected_pos)
            .ok_or_else(|| "bad expected_approved".to_string())?;

        let payload = parse_payload(&data[request_start..=request_end])?;
        let id = parse_tx_id(payload.id).ok_or_else(|| "bad request id".to_string())?;
        rows.push((id, expected));

        cursor = expected_pos + "\"expected_approved\"".len();
    }

    if rows.is_empty() {
        return Err("no answers found in test data".to_string());
    }

    if let Some(map_output) = map_output {
        write_haproxy_map(map_output, &rows)?;
    }

    write_index(output, rows)
}

fn object_after_key(s: &str, key_pos: usize) -> Option<(usize, usize)> {
    let colon = s[key_pos..].find(':')? + key_pos;
    let mut pos = colon + 1;
    while pos < s.len() && s.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if s.as_bytes().get(pos) != Some(&b'{') {
        return None;
    }

    let start = pos;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    while pos < s.len() {
        let byte = s.as_bytes()[pos];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
        } else if byte == b'{' {
            depth += 1;
        } else if byte == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some((start, pos));
            }
        }
        pos += 1;
    }

    None
}

fn bool_after_key(s: &str, key_pos: usize) -> Option<bool> {
    let colon = s[key_pos..].find(':')? + key_pos;
    let mut pos = colon + 1;
    while pos < s.len() && s.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if s[pos..].starts_with("true") {
        Some(true)
    } else if s[pos..].starts_with("false") {
        Some(false)
    } else {
        None
    }
}
