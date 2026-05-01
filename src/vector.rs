use crate::parser::Payload;

pub const DIM: usize = 14;
pub const SCALE: i32 = 10_000;
pub const K: usize = 5;
pub const BUCKET_COUNT: usize = 4096;

pub type QuantizedVector = [i16; DIM];

pub fn vectorize(payload: &Payload<'_>) -> QuantizedVector {
    let (year, month, day, hour, minute) =
        parse_time(payload.requested_at).unwrap_or((2026, 1, 1, 0, 0));
    let dow = day_of_week(year, month, day);

    let mut out = [0i16; DIM];
    out[0] = q(clamp01(payload.amount / 10_000.0));
    out[1] = q(clamp01(payload.installments / 12.0));
    out[2] = q(clamp01(amount_vs_avg(
        payload.amount,
        payload.customer_avg_amount,
    )));
    out[3] = q((hour as f64) / 23.0);
    out[4] = q((dow as f64) / 6.0);

    if let Some(last_ts) = payload.last_timestamp {
        let current = epoch_minutes(year, month, day, hour, minute);
        let last = parse_time(last_ts)
            .map(|(y, m, d, h, min)| epoch_minutes(y, m, d, h, min))
            .unwrap_or(current);
        let minutes = (current - last).max(0) as f64;
        out[5] = q(clamp01(minutes / 1440.0));
        out[6] = q(clamp01(
            payload.last_km_from_current.unwrap_or(0.0) / 1000.0,
        ));
    } else {
        out[5] = -SCALE as i16;
        out[6] = -SCALE as i16;
    }

    out[7] = q(clamp01(payload.km_from_home / 1000.0));
    out[8] = q(clamp01(payload.tx_count_24h / 20.0));
    out[9] = if payload.is_online { SCALE as i16 } else { 0 };
    out[10] = if payload.card_present {
        SCALE as i16
    } else {
        0
    };
    out[11] = if contains_quoted(payload.known_merchants, payload.merchant_id) {
        0
    } else {
        SCALE as i16
    };
    out[12] = q(mcc_risk(payload.mcc));
    out[13] = q(clamp01(payload.merchant_avg_amount / 10_000.0));
    out
}

pub fn quantize_reference(value: f64) -> i16 {
    if value <= -0.9999 {
        -SCALE as i16
    } else {
        q(clamp01(value))
    }
}

pub fn distance_sq(a: &QuantizedVector, b: &[i16]) -> i64 {
    let mut sum = 0i64;
    let mut i = 0;
    while i < DIM {
        let d = a[i] as i64 - b[i] as i64;
        sum += d * d;
        i += 1;
    }
    sum
}

pub fn bucket_key(v: &QuantizedVector) -> u16 {
    let amount = bucket8(v[0]);
    let ratio = bucket8(v[2]);
    let km_home = bucket8(v[7]);
    let hour = bucket4(v[3]);
    let no_last = if v[5] < 0 { 1 } else { 0 };

    (amount | (ratio << 3) | (km_home << 6) | (hour << 9) | (no_last << 11)) as u16
}

pub fn neighbor_keys(query: &QuantizedVector, out: &mut [u16; BUCKET_COUNT]) -> usize {
    let amount = bucket8(query[0]);
    let ratio = bucket8(query[2]);
    let km_home = bucket8(query[7]);
    let hour = bucket4(query[3]);
    let no_last = if query[5] < 0 { 1i32 } else { 0i32 };
    let mut seen = [false; BUCKET_COUNT];
    let mut n = 0usize;

    for radius in 0..8 {
        for a in (amount - radius).max(0)..=(amount + radius).min(7) {
            for r in (ratio - radius).max(0)..=(ratio + radius).min(7) {
                for k in (km_home - radius).max(0)..=(km_home + radius).min(7) {
                    for hr in (hour - radius).max(0)..=(hour + radius).min(3) {
                        let last_start = if radius >= 2 { 0 } else { no_last };
                        let last_end = if radius >= 2 { 1 } else { no_last };
                        for last in last_start..=last_end {
                            let key = (a | (r << 3) | (k << 6) | (hr << 9) | (last << 11)) as usize;
                            if !seen[key] {
                                seen[key] = true;
                                out[n] = key as u16;
                                n += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    n
}

fn amount_vs_avg(amount: f64, avg: f64) -> f64 {
    if avg <= 0.0 {
        1.0
    } else {
        (amount / avg) / 10.0
    }
}

fn clamp01(v: f64) -> f64 {
    if v.is_nan() {
        0.0
    } else {
        v.clamp(0.0, 1.0)
    }
}

fn q(v: f64) -> i16 {
    (v * SCALE as f64).round() as i16
}

fn bucket8(v: i16) -> i32 {
    if v <= 0 {
        0
    } else {
        ((v as i32 * 8) / (SCALE + 1)).clamp(0, 7)
    }
}

fn bucket4(v: i16) -> i32 {
    if v <= 0 {
        0
    } else {
        ((v as i32 * 4) / (SCALE + 1)).clamp(0, 3)
    }
}

fn mcc_risk(mcc: &str) -> f64 {
    match mcc {
        "5411" => 0.15,
        "5812" => 0.30,
        "5912" => 0.20,
        "5944" => 0.45,
        "7801" => 0.80,
        "7802" => 0.75,
        "7995" => 0.85,
        "4511" => 0.35,
        "5311" => 0.25,
        "5999" => 0.50,
        _ => 0.50,
    }
}

fn contains_quoted(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() || bytes.len() < needle.len() + 2 {
        return false;
    }
    let mut pos = 0usize;
    while pos + needle.len() + 2 <= bytes.len() {
        if bytes[pos] == b'"'
            && &bytes[pos + 1..pos + 1 + needle.len()] == needle
            && bytes[pos + 1 + needle.len()] == b'"'
        {
            return true;
        }
        pos += 1;
    }
    false
}

fn parse_time(ts: &str) -> Option<(i32, u32, u32, u32, u32)> {
    let b = ts.as_bytes();
    if b.len() < 16 {
        return None;
    }
    Some((
        parse_i32(&b[0..4])?,
        parse_u32(&b[5..7])?,
        parse_u32(&b[8..10])?,
        parse_u32(&b[11..13])?,
        parse_u32(&b[14..16])?,
    ))
}

fn parse_i32(bytes: &[u8]) -> Option<i32> {
    parse_u32(bytes).map(|v| v as i32)
}

fn parse_u32(bytes: &[u8]) -> Option<u32> {
    let mut n = 0u32;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (b - b'0') as u32;
    }
    Some(n)
}

fn day_of_week(y: i32, m: u32, d: u32) -> u32 {
    const T: [i32; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let mut yy = y;
    if m < 3 {
        yy -= 1;
    }
    let dow = (yy + yy / 4 - yy / 100 + yy / 400 + T[(m - 1) as usize] + d as i32) % 7;
    ((dow + 6) % 7) as u32
}

fn epoch_minutes(y: i32, m: u32, d: u32, h: u32, min: u32) -> i64 {
    days_from_civil(y, m, d) * 1440 + h as i64 * 60 + min as i64
}

fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m as i32 + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe - 719468) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_payload;

    #[test]
    fn vectorizes_payload_without_last_transaction() {
        let payload =
            parse_payload(include_str!("../resources/example-payload-legit.json")).unwrap();
        let vector = vectorize(&payload);

        assert_eq!(vector[0], 41);
        assert_eq!(vector[1], 1667);
        assert_eq!(vector[2], 500);
        assert_eq!(vector[3], 7826);
        assert_eq!(vector[4], 3333);
        assert_eq!(vector[5], -10000);
        assert_eq!(vector[6], -10000);
        assert_eq!(vector[9], 0);
        assert_eq!(vector[10], 10000);
        assert_eq!(vector[11], 0);
        assert_eq!(vector[12], 1500);
    }

    #[test]
    fn vectorizes_unknown_merchant_and_high_risk_mcc() {
        let payload =
            parse_payload(include_str!("../resources/example-payload-fraud.json")).unwrap();
        let vector = vectorize(&payload);

        assert_eq!(vector[0], 9506);
        assert_eq!(vector[1], 8333);
        assert_eq!(vector[2], 10000);
        assert_eq!(vector[3], 2174);
        assert_eq!(vector[4], 8333);
        assert_eq!(vector[11], 10000);
        assert_eq!(vector[12], 7500);
    }
}
