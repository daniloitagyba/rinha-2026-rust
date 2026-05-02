use crate::index::{exact_fallback_name, DecisionKind, Index, SearchParams};
use crate::parser::parse_payload;
use crate::vector::vectorize;
use crate::vector::K;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::time::Instant;

pub fn run(input: &str) -> Result<(), String> {
    let index_path = env::var("INDEX_PATH").unwrap_or_else(|_| "data/references.idx".to_string());
    let limit = env_usize("EVAL_LIMIT", usize::MAX);
    let errors_path = env::var("EVAL_ERRORS_PATH").ok();
    let dump_path = env::var("EVAL_DUMP_PATH").ok();
    let data = fs::read_to_string(input).map_err(|e| format!("failed to read {input}: {e}"))?;
    let index = Index::open(&index_path)?;
    let params = SearchParams::from_env();
    let mut error_writer = optional_writer(errors_path.as_deref())?;
    let mut dump_writer = optional_writer(dump_path.as_deref())?;

    let mut cursor = 0usize;
    let mut total = 0usize;
    let mut correct = 0usize;
    let mut fp = 0usize;
    let mut fn_ = 0usize;
    let mut parse_errors = 0usize;
    let mut kind_counts = [0usize; 5];
    let mut fraud_count_buckets = [0usize; K + 1];
    let mut latencies_ns = Vec::new();
    let started = Instant::now();

    while total < limit {
        let Some(request_key_pos) = data[cursor..].find("\"request\"") else {
            break;
        };
        let request_key_pos = cursor + request_key_pos;
        let Some((request_start, request_end)) = object_after_key(&data, request_key_pos) else {
            parse_errors += 1;
            cursor = request_key_pos + "\"request\"".len();
            continue;
        };
        let expected_pos = data[request_end..]
            .find("\"expected_approved\"")
            .map(|pos| request_end + pos)
            .ok_or_else(|| "missing expected_approved".to_string())?;
        let expected = bool_after_key(&data, expected_pos)
            .ok_or_else(|| "bad expected_approved".to_string())?;

        let item_started = Instant::now();
        match parse_payload(&data[request_start..=request_end]) {
            Ok(payload) => {
                let query = vectorize(&payload);
                let (approved, score, kind) = index.classify_detailed(&query, &params);
                let fraud_count = fraud_count_from_score(score);
                latencies_ns.push(item_started.elapsed().as_nanos());
                kind_counts[kind_index(kind)] += 1;
                fraud_count_buckets[fraud_count] += 1;

                if approved == expected {
                    correct += 1;
                } else if approved {
                    fn_ += 1;
                } else {
                    fp += 1;
                }

                if let Some(writer) = dump_writer.as_mut() {
                    write_eval_row(
                        writer,
                        expected,
                        approved,
                        fraud_count,
                        kind,
                        Some(&query),
                        None,
                    )?;
                }

                if approved != expected {
                    if let Some(writer) = error_writer.as_mut() {
                        write_eval_row(
                            writer,
                            expected,
                            approved,
                            fraud_count,
                            kind,
                            Some(&query),
                            Some(&data[request_start..=request_end]),
                        )?;
                    }
                }
            }
            Err(_) => {
                parse_errors += 1;
            }
        }

        total += 1;
        cursor = expected_pos + "\"expected_approved\"".len();
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
        (fp + fn_ + parse_errors) as f64 / total as f64
    };
    let epsilon = if total == 0 {
        0.0
    } else {
        weighted_errors as f64 / total as f64
    };
    let score_det = detection_score(weighted_errors, failure_rate, epsilon);

    println!("index={index_path}");
    println!("risky_fallback_refs={}", index.risky_fallback_count());
    println!(
        "params early_candidates={} min_candidates={} max_candidates={} profile_fastpath={} profile_min_count={} exact_fallback={} overload_min_candidates={} overload_max_candidates={} overload_threshold={} overload_fast_only={} search_fallback_last_distance={} flat={} fast_path={} fast_only={}",
        params.early_candidates,
        params.min_candidates,
        params.max_candidates,
        params.profile_fast_path,
        params.profile_min_count,
        exact_fallback_name(params.exact_fallback),
        params.overload_min_candidates,
        params.overload_max_candidates,
        params.overload_threshold,
        params.overload_fast_only,
        params.search_fallback_last_distance,
        params.flat,
        params.fast_path,
        params.fast_only
    );
    println!("total={total} measured={measured} correct={correct} accuracy={accuracy:.6}");
    println!(
        "fp={fp} fn={fn_} parse_errors={parse_errors} weighted_errors={weighted_errors} failure_rate={failure_rate:.6} score_det={score_det:.2}"
    );
    println!(
        "elapsed_ms={} throughput_per_s={throughput:.1}",
        elapsed.as_millis()
    );
    println!("classify_latency_ns p50={p50} p95={p95} p99={p99}");
    println!(
        "decision_counts profile_fast={} rule_fast={} approx={} exact_flat={} exact_risky={}",
        kind_counts[0], kind_counts[1], kind_counts[2], kind_counts[3], kind_counts[4]
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

fn detection_score(weighted_errors: usize, failure_rate: f64, epsilon: f64) -> f64 {
    if failure_rate > 0.15 {
        return -3000.0;
    }
    let safe_epsilon = epsilon.max(0.001);
    1000.0 * (1.0 / safe_epsilon).log10() - 300.0 * (1.0 + weighted_errors as f64).log10()
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
        DecisionKind::ExactRisky => 4,
    }
}

fn write_eval_row(
    writer: &mut BufWriter<File>,
    expected_approved: bool,
    approved: bool,
    fraud_count: usize,
    kind: DecisionKind,
    vector: Option<&[i16; 14]>,
    request: Option<&str>,
) -> Result<(), String> {
    write!(
        writer,
        "{{\"expected_approved\":{},\"approved\":{},\"fraud_count\":{},\"decision\":\"{}\"",
        expected_approved,
        approved,
        fraud_count,
        kind.as_str()
    )
    .map_err(|e| e.to_string())?;

    if let Some(vector) = vector {
        writer
            .write_all(b",\"vector\":[")
            .map_err(|e| e.to_string())?;
        for (idx, value) in vector.iter().enumerate() {
            if idx > 0 {
                writer.write_all(b",").map_err(|e| e.to_string())?;
            }
            write!(writer, "{value}").map_err(|e| e.to_string())?;
        }
        writer.write_all(b"]").map_err(|e| e.to_string())?;
    }

    if let Some(request) = request {
        writer
            .write_all(b",\"request\":")
            .map_err(|e| e.to_string())?;
        writer
            .write_all(request.as_bytes())
            .map_err(|e| e.to_string())?;
    }

    writer.write_all(b"}\n").map_err(|e| e.to_string())
}
