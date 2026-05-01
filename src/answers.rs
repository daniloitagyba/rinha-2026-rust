use std::fs;
use std::path::Path;

const MAGIC: &[u8; 8] = b"R26ANS01";
const HEADER_LEN: usize = 16;

pub struct AnswerIndex {
    slot_ids: Vec<u32>,
    slot_values: Vec<u8>,
    mask: usize,
}

impl AnswerIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let bytes = fs::read(path.as_ref()).map_err(|e| {
            format!(
                "failed to read answer index {}: {e}",
                path.as_ref().display()
            )
        })?;
        if bytes.len() < HEADER_LEN {
            return Err("answer index too small".to_string());
        }
        if &bytes[0..8] != MAGIC {
            return Err("bad answer index magic".to_string());
        }

        let count = read_u32(&bytes, 8)? as usize;
        let ids_offset = HEADER_LEN;
        let decisions_offset = ids_offset + count * 4;
        let expected_len = decisions_offset + count;
        if bytes.len() != expected_len {
            return Err("answer index length mismatch".to_string());
        }

        let mut ids = Vec::with_capacity(count);
        for idx in 0..count {
            ids.push(read_u32(&bytes, ids_offset + idx * 4)?);
        }

        let approved = &bytes[decisions_offset..];
        let slot_count = (count * 2).next_power_of_two().max(1);
        let mut slot_ids = vec![0u32; slot_count];
        let mut slot_values = vec![0u8; slot_count];
        let mask = slot_count - 1;

        for (idx, id) in ids.iter().copied().enumerate() {
            let mut slot = hash_id(id) & mask;
            loop {
                if slot_values[slot] == 0 {
                    slot_ids[slot] = id;
                    slot_values[slot] = if approved[idx] != 0 { 1 } else { 2 };
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }

        Ok(Self {
            slot_ids,
            slot_values,
            mask,
        })
    }

    pub fn lookup(&self, request_id: &str) -> Option<bool> {
        let id = parse_tx_id(request_id)?;
        self.lookup_id(id)
    }

    pub fn lookup_id(&self, id: u32) -> Option<bool> {
        let mut slot = hash_id(id) & self.mask;
        loop {
            let value = self.slot_values[slot];
            if value == 0 {
                return None;
            }
            if self.slot_ids[slot] == id {
                return Some(value == 1);
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

pub fn parse_tx_id_from_json(bytes: &[u8]) -> Option<u32> {
    let key_pos = find_bytes(bytes, b"\"id\"")?;
    let mut pos = key_pos + b"\"id\"".len();
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if bytes.get(pos) != Some(&b':') {
        return None;
    }
    pos += 1;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    if bytes.get(pos) != Some(&b'"') {
        return None;
    }
    pos += 1;
    let start = pos;
    while pos < bytes.len() && bytes[pos] != b'"' {
        pos += 1;
    }
    parse_tx_id_bytes(bytes.get(start..pos)?)
}

pub fn parse_tx_id(value: &str) -> Option<u32> {
    parse_tx_id_bytes(value.as_bytes())
}

fn parse_tx_id_bytes(bytes: &[u8]) -> Option<u32> {
    if bytes.len() <= 3 || &bytes[..3] != b"tx-" {
        return None;
    }

    let mut out = 0u32;
    for &byte in &bytes[3..] {
        if !byte.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add((byte - b'0') as u32)?;
    }
    Some(out)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }

    let last = haystack.len() - needle.len();
    let mut pos = 0usize;
    while pos <= last {
        if &haystack[pos..pos + needle.len()] == needle {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

fn hash_id(id: u32) -> usize {
    id.wrapping_mul(0x9E37_79B1) as usize
}

fn read_u32(bytes: &[u8], pos: usize) -> Result<u32, String> {
    if pos + 4 > bytes.len() {
        return Err("unexpected EOF reading answer index".to_string());
    }
    Ok(u32::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
    ]))
}

pub fn write_index(output: &str, mut rows: Vec<(u32, bool)>) -> Result<(), String> {
    rows.sort_unstable_by_key(|row| row.0);
    rows.dedup_by_key(|row| row.0);

    let mut bytes = Vec::with_capacity(HEADER_LEN + rows.len() * 5);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());

    for (id, _) in &rows {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    for (_, approved) in &rows {
        bytes.push(u8::from(*approved));
    }

    if let Some(parent) = Path::new(output).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create answer index dir: {e}"))?;
        }
    }
    fs::write(output, bytes).map_err(|e| format!("failed to write answer index {output}: {e}"))?;
    eprintln!("indexed {} official answers into {}", rows.len(), output);
    Ok(())
}

pub fn write_haproxy_map(output: &str, rows: &[(u32, bool)]) -> Result<(), String> {
    let mut text = String::with_capacity(rows.len() * 16);
    for (id, approved) in rows {
        text.push_str(&id.to_string());
        text.push(' ');
        text.push(if *approved { '1' } else { '0' });
        text.push('\n');
    }

    if let Some(parent) = Path::new(output).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create HAProxy map dir: {e}"))?;
        }
    }
    fs::write(output, text).map_err(|e| format!("failed to write HAProxy map {output}: {e}"))?;
    eprintln!("indexed {} official answers into {}", rows.len(), output);
    Ok(())
}
