use crate::vector::{bucket_key, quantize_reference, QuantizedVector, BUCKET_COUNT, DIM, SCALE};
use std::fs::{create_dir_all, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"RINHA26I";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 80;

pub fn run(output: &str) -> Result<(), String> {
    if let Some(parent) = Path::new(output).parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).map_err(|e| format!("failed to create index dir: {e}"))?;
        }
    }

    let mut file = File::create(output).map_err(|e| format!("failed to create {output}: {e}"))?;
    file.write_all(&[0u8; HEADER_LEN])
        .map_err(|e| format!("failed to reserve header: {e}"))?;

    let mut vectors: Vec<QuantizedVector> = Vec::new();
    let mut labels: Vec<u8> = Vec::new();
    let mut keys: Vec<u16> = Vec::new();
    let mut bucket_counts = [0u32; BUCKET_COUNT];
    let stdin = io::stdin();
    let mut scanner = JsonScanner::new(stdin.lock());

    while scanner
        .find_bytes(b"\"vector\"")
        .map_err(|e| format!("failed to read references JSON from stdin: {e}"))?
    {
        scanner.expect_until(b'[')?;
        let mut qvec = [0i16; DIM];

        for slot in qvec.iter_mut().take(DIM) {
            scanner.skip_ws_and_commas()?;
            let value = scanner.read_number()?;
            *slot = quantize_reference(value);
        }

        scanner.find_required(b"\"label\"")?;
        scanner.expect_until(b':')?;
        scanner.skip_ws()?;
        let label = scanner.read_label()?;
        let key = bucket_key(&qvec) as usize;
        bucket_counts[key] += 1;

        vectors.push(qvec);
        labels.push(label);
        keys.push(key as u16);
    }

    if labels.is_empty() {
        return Err("no reference vectors found".to_string());
    }

    let mut offsets = vec![0u32; BUCKET_COUNT + 1];
    for i in 0..BUCKET_COUNT {
        offsets[i + 1] = offsets[i] + bucket_counts[i];
    }

    let mut write_positions = offsets[..BUCKET_COUNT].to_vec();
    let mut items = vec![0u32; labels.len()];
    for (idx, key) in keys.iter().enumerate() {
        let pos = write_positions[*key as usize] as usize;
        items[pos] = idx as u32;
        write_positions[*key as usize] += 1;
    }

    let vectors_offset = file.stream_position().map_err(|e| e.to_string())?;
    for original_id in &items {
        let vector = &vectors[*original_id as usize];
        for value in vector {
            file.write_all(&value.to_le_bytes())
                .map_err(|e| format!("failed to write vectors: {e}"))?;
        }
    }

    let labels_offset = file.stream_position().map_err(|e| e.to_string())?;
    for original_id in &items {
        file.write_all(&[labels[*original_id as usize]])
            .map_err(|e| format!("failed to write labels: {e}"))?;
    }

    let bucket_offsets_offset = file.stream_position().map_err(|e| e.to_string())?;
    for value in &offsets {
        file.write_all(&value.to_le_bytes())
            .map_err(|e| format!("failed to write bucket offsets: {e}"))?;
    }

    let bucket_items_offset = file.stream_position().map_err(|e| e.to_string())?;
    for value in 0..labels.len() as u32 {
        file.write_all(&value.to_le_bytes())
            .map_err(|e| format!("failed to write bucket items: {e}"))?;
    }

    let file_len = file.stream_position().map_err(|e| e.to_string())?;
    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    write_header(
        &mut file,
        labels.len() as u32,
        vectors_offset,
        labels_offset,
        bucket_offsets_offset,
        bucket_items_offset,
        file_len,
    )?;
    eprintln!(
        "indexed {} vectors into {} ({} clustered buckets, {} bytes)",
        labels.len(),
        output,
        BUCKET_COUNT,
        file_len
    );
    Ok(())
}

fn write_header(
    file: &mut File,
    count: u32,
    vectors_offset: u64,
    labels_offset: u64,
    bucket_offsets_offset: u64,
    bucket_items_offset: u64,
    file_len: u64,
) -> Result<(), String> {
    file.write_all(MAGIC).map_err(|e| e.to_string())?;
    write_u32(file, VERSION)?;
    write_u32(file, DIM as u32)?;
    write_u32(file, count)?;
    write_u32(file, SCALE as u32)?;
    write_u32(file, BUCKET_COUNT as u32)?;
    write_u32(file, 0)?;
    write_u64(file, vectors_offset)?;
    write_u64(file, labels_offset)?;
    write_u64(file, bucket_offsets_offset)?;
    write_u64(file, bucket_items_offset)?;
    write_u64(file, file_len)?;
    write_u64(file, 0)?;
    Ok(())
}

fn write_u32(file: &mut File, value: u32) -> Result<(), String> {
    file.write_all(&value.to_le_bytes())
        .map_err(|e| format!("failed to write header: {e}"))
}

fn write_u64(file: &mut File, value: u64) -> Result<(), String> {
    file.write_all(&value.to_le_bytes())
        .map_err(|e| format!("failed to write header: {e}"))
}

struct JsonScanner<R: Read> {
    reader: BufReader<R>,
    pushed: Option<u8>,
}

impl<R: Read> JsonScanner<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(64 * 1024, reader),
            pushed: None,
        }
    }

    fn find_required(&mut self, needle: &[u8]) -> Result<(), String> {
        if self.find_bytes(needle).map_err(|e| e.to_string())? {
            Ok(())
        } else {
            Err("unexpected EOF while scanning JSON".to_string())
        }
    }

    fn find_bytes(&mut self, needle: &[u8]) -> io::Result<bool> {
        let mut matched = 0usize;
        loop {
            let Some(byte) = self.read_byte()? else {
                return Ok(false);
            };
            if byte == needle[matched] {
                matched += 1;
                if matched == needle.len() {
                    return Ok(true);
                }
            } else {
                matched = if byte == needle[0] { 1 } else { 0 };
            }
        }
    }

    fn expect_until(&mut self, expected: u8) -> Result<(), String> {
        loop {
            match self.read_byte().map_err(|e| e.to_string())? {
                Some(byte) if byte == expected => return Ok(()),
                Some(_) => {}
                None => return Err("unexpected EOF while scanning JSON".to_string()),
            }
        }
    }

    fn skip_ws(&mut self) -> Result<(), String> {
        loop {
            match self.read_byte().map_err(|e| e.to_string())? {
                Some(byte) if byte.is_ascii_whitespace() => {}
                Some(byte) => {
                    self.push(byte);
                    return Ok(());
                }
                None => return Ok(()),
            }
        }
    }

    fn skip_ws_and_commas(&mut self) -> Result<(), String> {
        loop {
            match self.read_byte().map_err(|e| e.to_string())? {
                Some(byte) if byte.is_ascii_whitespace() || byte == b',' => {}
                Some(byte) => {
                    self.push(byte);
                    return Ok(());
                }
                None => return Err("unexpected EOF while reading vector".to_string()),
            }
        }
    }

    fn read_number(&mut self) -> Result<f64, String> {
        let mut bytes = [0u8; 64];
        let mut len = 0usize;

        loop {
            match self.read_byte().map_err(|e| e.to_string())? {
                Some(byte)
                    if byte.is_ascii_digit()
                        || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E') =>
                {
                    if len >= bytes.len() {
                        return Err("numeric token too long".to_string());
                    }
                    bytes[len] = byte;
                    len += 1;
                }
                Some(byte) => {
                    self.push(byte);
                    break;
                }
                None => break,
            }
        }

        if len == 0 {
            return Err("expected number".to_string());
        }
        let token =
            std::str::from_utf8(&bytes[..len]).map_err(|_| "bad numeric token".to_string())?;
        token
            .parse::<f64>()
            .map_err(|e| format!("bad numeric token {token}: {e}"))
    }

    fn read_label(&mut self) -> Result<u8, String> {
        let text = self.read_string()?;
        match text.as_slice() {
            b"fraud" => Ok(1),
            b"legit" => Ok(0),
            _ => Err("unknown label".to_string()),
        }
    }

    fn read_string(&mut self) -> Result<Vec<u8>, String> {
        match self.read_byte().map_err(|e| e.to_string())? {
            Some(b'"') => {}
            _ => return Err("expected string".to_string()),
        }

        let mut out = Vec::with_capacity(16);
        let mut escaped = false;
        loop {
            match self.read_byte().map_err(|e| e.to_string())? {
                Some(byte) if escaped => {
                    out.push(byte);
                    escaped = false;
                }
                Some(b'\\') => escaped = true,
                Some(b'"') => return Ok(out),
                Some(byte) => out.push(byte),
                None => return Err("unexpected EOF while reading string".to_string()),
            }
        }
    }

    fn read_byte(&mut self) -> io::Result<Option<u8>> {
        if let Some(byte) = self.pushed.take() {
            return Ok(Some(byte));
        }
        let mut byte = [0u8; 1];
        match self.reader.read(&mut byte)? {
            0 => Ok(None),
            _ => Ok(Some(byte[0])),
        }
    }

    fn push(&mut self, byte: u8) {
        self.pushed = Some(byte);
    }
}
