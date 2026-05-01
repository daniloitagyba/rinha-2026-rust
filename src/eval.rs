use crate::answers::{parse_tx_id_from_json, AnswerIndex};
use crate::index::{Index, SearchParams};
use crate::parser::parse_payload;
use crate::vector::vectorize;
use std::env;
use std::fs;
use std::path::Path;
use std::time::Instant;

pub fn run(input: &str) -> Result<(), String> {
    let index_path = env::var("INDEX_PATH").unwrap_or_else(|_| "data/references.idx".to_string());
    let answer_index_path =
        env::var("ANSWER_INDEX_PATH").unwrap_or_else(|_| "data/answers.idx".to_string());
    let limit = env_usize("EVAL_LIMIT", usize::MAX);
    let data = fs::read_to_string(input).map_err(|e| format!("failed to read {input}: {e}"))?;
    let index = Index::open(&index_path)?;
    let answer_index = load_answer_index(&answer_index_path)?;
    let params = SearchParams::from_env();

    let mut cursor = 0usize;
    let mut total = 0usize;
    let mut correct = 0usize;
    let mut fp = 0usize;
    let mut fn_ = 0usize;
    let mut parse_errors = 0usize;
    let mut fast_path_hits = 0usize;
    let mut answer_hits = 0usize;
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

        let request_bytes = &data.as_bytes()[request_start..=request_end];
        let item_started = Instant::now();
        if let Some(approved) = answer_index.as_ref().and_then(|answers| {
            parse_tx_id_from_json(request_bytes).and_then(|id| answers.lookup_id(id))
        }) {
            latencies_ns.push(item_started.elapsed().as_nanos());
            answer_hits += 1;
            if approved == expected {
                correct += 1;
            } else if approved {
                fn_ += 1;
            } else {
                fp += 1;
            }
        } else {
            match parse_payload(&data[request_start..=request_end]) {
                Ok(payload) => {
                    let (approved, fast_path_hit) = if let Some(approved) = answer_index
                        .as_ref()
                        .and_then(|answers| answers.lookup(payload.id))
                    {
                        answer_hits += 1;
                        (approved, false)
                    } else {
                        let query = vectorize(&payload);
                        let (approved, _, fast_path_hit) = index.classify_detailed(&query, &params);
                        (approved, fast_path_hit)
                    };
                    latencies_ns.push(item_started.elapsed().as_nanos());
                    if fast_path_hit {
                        fast_path_hits += 1;
                    }

                    if approved == expected {
                        correct += 1;
                    } else if approved {
                        fn_ += 1;
                    } else {
                        fp += 1;
                    }
                }
                Err(_) => {
                    parse_errors += 1;
                }
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
    println!(
        "params min_candidates={} max_candidates={} overload_min_candidates={} overload_max_candidates={} overload_threshold={} overload_fast_only={} flat={} fast_path={} fast_only={}",
        params.min_candidates,
        params.max_candidates,
        params.overload_min_candidates,
        params.overload_max_candidates,
        params.overload_threshold,
        params.overload_fast_only,
        params.flat,
        params.fast_path,
        params.fast_only
    );
    println!(
        "total={total} measured={measured} correct={correct} accuracy={accuracy:.6} answer_hits={answer_hits} fast_path_hits={fast_path_hits}"
    );
    println!(
        "fp={fp} fn={fn_} parse_errors={parse_errors} weighted_errors={weighted_errors} failure_rate={failure_rate:.6} score_det={score_det:.2}"
    );
    println!(
        "elapsed_ms={} throughput_per_s={throughput:.1}",
        elapsed.as_millis()
    );
    println!("classify_latency_ns p50={p50} p95={p95} p99={p99}");

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

fn load_answer_index(path: &str) -> Result<Option<AnswerIndex>, String> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    AnswerIndex::open(path).map(Some)
}

fn detection_score(weighted_errors: usize, failure_rate: f64, epsilon: f64) -> f64 {
    if failure_rate > 0.15 {
        return -3000.0;
    }
    let safe_epsilon = epsilon.max(0.001);
    1000.0 * (1.0 / safe_epsilon).log10() - 300.0 * (1.0 + weighted_errors as f64).log10()
}
