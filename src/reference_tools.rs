use crate::index::{exact_fallback_name, DecisionKind, Index, SearchParams};
use crate::vector::{quantize_reference, QuantizedVector, DIM, K};
use std::env;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::time::Instant;

pub fn split_references(
    train_path: &str,
    holdout_path: &str,
    modulus: usize,
    offset: usize,
) -> Result<(), String> {
    if modulus < 2 {
        return Err("modulus must be greater than 1".to_string());
    }
    if offset >= modulus {
        return Err("offset must be smaller than modulus".to_string());
    }

    let stdin = io::stdin();
    let mut reader = BufReader::with_capacity(64 * 1024, stdin.lock());
    let mut train = BufWriter::with_capacity(
        1024 * 1024,
        File::create(train_path).map_err(|e| format!("failed to create {train_path}: {e}"))?,
    );
    let mut holdout = BufWriter::with_capacity(
        1024 * 1024,
        File::create(holdout_path).map_err(|e| format!("failed to create {holdout_path}: {e}"))?,
    );

    train.write_all(b"[").map_err(|e| e.to_string())?;
    holdout.write_all(b"[").map_err(|e| e.to_string())?;

    let mut object = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut total = 0usize;
    let mut train_count = 0usize;
    let mut holdout_count = 0usize;

    loop {
        let n = reader
            .read(&mut byte)
            .map_err(|e| format!("failed to read references: {e}"))?;
        if n == 0 {
            break;
        }
        let b = byte[0];

        if depth == 0 {
            if b == b'{' {
                object.clear();
                object.push(b);
                depth = 1;
                in_string = false;
                escaped = false;
            }
            continue;
        }

        object.push(b);
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }

        match b {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    if total % modulus == offset {
                        if holdout_count > 0 {
                            holdout.write_all(b",").map_err(|e| e.to_string())?;
                        }
                        holdout.write_all(&object).map_err(|e| e.to_string())?;
                        holdout_count += 1;
                    } else {
                        if train_count > 0 {
                            train.write_all(b",").map_err(|e| e.to_string())?;
                        }
                        train.write_all(&object).map_err(|e| e.to_string())?;
                        train_count += 1;
                    }
                    total += 1;
                }
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err("unexpected EOF while splitting reference object".to_string());
    }

    train.write_all(b"]\n").map_err(|e| e.to_string())?;
    holdout.write_all(b"]\n").map_err(|e| e.to_string())?;
    train.flush().map_err(|e| e.to_string())?;
    holdout.flush().map_err(|e| e.to_string())?;
    eprintln!(
        "split {total} references into train={train_count} holdout={holdout_count} modulus={modulus} offset={offset}"
    );
    Ok(())
}

pub fn eval_references(input: &str) -> Result<(), String> {
    let index_path = env::var("INDEX_PATH").unwrap_or_else(|_| "data/references.idx".to_string());
    let limit = env_usize("EVAL_LIMIT", usize::MAX);
    let errors_path = env::var("EVAL_ERRORS_PATH").ok();
    let dump_path = env::var("EVAL_DUMP_PATH").ok();
    let index = Index::open(&index_path)?;
    let params = SearchParams::from_env();
    let file = File::open(input).map_err(|e| format!("failed to open {input}: {e}"))?;
    let mut scanner = JsonScanner::new(file);
    let mut error_writer = optional_writer(errors_path.as_deref())?;
    let mut dump_writer = optional_writer(dump_path.as_deref())?;

    let mut total = 0usize;
    let mut correct = 0usize;
    let mut fp = 0usize;
    let mut fn_ = 0usize;
    let mut kind_counts = [0usize; 6];
    let mut fraud_count_buckets = [0usize; K + 1];
    let mut latencies_ns = Vec::new();
    let started = Instant::now();

    while total < limit
        && scanner
            .find_bytes(b"\"vector\"")
            .map_err(|e| format!("failed to read {input}: {e}"))?
    {
        scanner.expect_until(b'[')?;
        let mut query: QuantizedVector = [0; DIM];
        for value in query.iter_mut().take(DIM) {
            scanner.skip_ws_and_commas()?;
            *value = quantize_reference(scanner.read_number()?);
        }
        scanner.find_required(b"\"label\"")?;
        scanner.expect_until(b':')?;
        scanner.skip_ws()?;
        let label = scanner.read_label()?;
        let expected_approved = label == 0;

        let item_started = Instant::now();
        let (approved, score, kind) = index.classify_detailed(&query, &params);
        latencies_ns.push(item_started.elapsed().as_nanos());
        let fraud_count = fraud_count_from_score(score);
        kind_counts[kind_index(kind)] += 1;
        fraud_count_buckets[fraud_count] += 1;

        if approved == expected_approved {
            correct += 1;
        } else if approved {
            fn_ += 1;
        } else {
            fp += 1;
        }

        if let Some(writer) = dump_writer.as_mut() {
            write_eval_row(
                writer,
                expected_approved,
                approved,
                fraud_count,
                kind,
                &query,
            )?;
        }
        if approved != expected_approved {
            if let Some(writer) = error_writer.as_mut() {
                write_eval_row(
                    writer,
                    expected_approved,
                    approved,
                    fraud_count,
                    kind,
                    &query,
                )?;
            }
        }

        total += 1;
    }

    let elapsed = started.elapsed();
    latencies_ns.sort_unstable();
    let measured = latencies_ns.len();
    let p50 = percentile(&latencies_ns, 0.50);
    let p95 = percentile(&latencies_ns, 0.95);
    let p99 = percentile(&latencies_ns, 0.99);
    let accuracy = if total == 0 {
        0.0
    } else {
        correct as f64 / total as f64
    };
    let throughput = if elapsed.as_secs_f64() == 0.0 {
        0.0
    } else {
        total as f64 / elapsed.as_secs_f64()
    };
    let weighted_errors = fp + 3 * fn_;
    let failure_rate = if total == 0 {
        0.0
    } else {
        (fp + fn_) as f64 / total as f64
    };

    println!("index={index_path}");
    println!(
        "references_gzip_sha256={}",
        index
            .references_gzip_sha256_hex()
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "references_json_sha256={}",
        index
            .references_json_sha256_hex()
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "profile_fastpaths_allowed={}",
        index.profile_fast_paths_allowed()
    );
    println!("risky_fallback_refs={}", index.risky_fallback_count());
    println!(
        "params early_candidates={} min_candidates={} max_candidates={} profile_fastpath={} profile_min_count={} profile_legit_min_count={} profile_fraud_min_count={} profile_dominant_fastpath={} profile_dominant_min_count={} profile_dominant_max_opposite={} early_edge_fallback={} exact_fallback={} profile_exact_triggers={} risky_semantic_groups={} risky_semantic_radius={} flat={} fast_path={} fast_only={}",
        params.early_candidates,
        params.min_candidates,
        params.max_candidates,
        params.profile_fast_path,
        params.profile_min_count,
        params.profile_legit_min_count,
        params.profile_fraud_min_count,
        params.profile_dominant_fast_path,
        params.profile_dominant_min_count,
        params.profile_dominant_max_opposite,
        params.early_edge_fallback,
        exact_fallback_name(params.exact_fallback),
        params.profile_exact_triggers,
        params.risky_semantic_groups,
        params.risky_semantic_radius,
        params.flat,
        params.fast_path,
        params.fast_only
    );
    println!("total={total} measured={measured} correct={correct} accuracy={accuracy:.6}");
    println!("fp={fp} fn={fn_} weighted_errors={weighted_errors} failure_rate={failure_rate:.6}");
    println!(
        "elapsed_ms={} throughput_per_s={throughput:.1}",
        elapsed.as_millis()
    );
    println!("classify_latency_ns p50={p50} p95={p95} p99={p99}");
    println!(
        "decision_counts profile_fast={} rule_fast={} approx={} exact_flat={} exact_risky_flat={} exact_risky_bucket={}",
        kind_counts[0], kind_counts[1], kind_counts[2], kind_counts[3], kind_counts[4], kind_counts[5]
    );
    println!(
        "fraud_count_buckets 0={} 1={} 2={} 3={} 4={} 5={}",
        fraud_count_buckets[0],
        fraud_count_buckets[1],
        fraud_count_buckets[2],
        fraud_count_buckets[3],
        fraud_count_buckets[4],
        fraud_count_buckets[5]
    );
    Ok(())
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

fn percentile(sorted: &[u128], q: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn optional_writer(path: Option<&str>) -> Result<Option<BufWriter<File>>, String> {
    match path {
        Some(path) if !path.trim().is_empty() => {
            let file = File::create(path).map_err(|e| format!("failed to create {path}: {e}"))?;
            Ok(Some(BufWriter::with_capacity(64 * 1024, file)))
        }
        _ => Ok(None),
    }
}

fn fraud_count_from_score(score: f32) -> usize {
    ((score * K as f32).round() as usize).min(K)
}

fn kind_index(kind: DecisionKind) -> usize {
    match kind {
        DecisionKind::ProfileFast => 0,
        DecisionKind::RuleFast => 1,
        DecisionKind::Approx => 2,
        DecisionKind::ExactFlat => 3,
        DecisionKind::ExactRiskyFlat => 4,
        DecisionKind::ExactRiskyBucket => 5,
    }
}

fn write_eval_row(
    writer: &mut BufWriter<File>,
    expected_approved: bool,
    approved: bool,
    fraud_count: usize,
    kind: DecisionKind,
    vector: &QuantizedVector,
) -> Result<(), String> {
    write!(
        writer,
        "{{\"expected_approved\":{},\"approved\":{},\"fraud_count\":{},\"decision\":\"{}\",\"vector\":[",
        expected_approved,
        approved,
        fraud_count,
        kind.as_str()
    )
    .map_err(|e| e.to_string())?;

    for (idx, value) in vector.iter().enumerate() {
        if idx > 0 {
            writer.write_all(b",").map_err(|e| e.to_string())?;
        }
        write!(writer, "{value}").map_err(|e| e.to_string())?;
    }

    writer.write_all(b"]}\n").map_err(|e| e.to_string())
}
