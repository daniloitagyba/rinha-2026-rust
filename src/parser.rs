pub struct Payload<'a> {
    pub amount: f64,
    pub installments: f64,
    pub requested_at: &'a [u8],
    pub customer_avg_amount: f64,
    pub tx_count_24h: f64,
    pub known_merchants: &'a [u8],
    pub merchant_id: &'a [u8],
    pub mcc: &'a [u8],
    pub merchant_avg_amount: f64,
    pub is_online: bool,
    pub card_present: bool,
    pub km_from_home: f64,
    pub last_timestamp: Option<&'a [u8]>,
    pub last_km_from_current: Option<f64>,
}

pub fn parse_payload(body: &[u8]) -> Result<Payload<'_>, &'static str> {
    if let Some(payload) = parse_payload_ordered(body) {
        return Ok(payload);
    }
    parse_payload_scan(body)
}

fn parse_payload_ordered(body: &[u8]) -> Option<Payload<'_>> {
    let mut pos = 0usize;

    consume_byte(body, &mut pos, b'{')?;
    consume_key(body, &mut pos, b"\"id\"")?;
    skip_json_string(body, &mut pos)?;
    consume_comma(body, &mut pos)?;

    consume_key(body, &mut pos, b"\"transaction\"")?;
    consume_byte(body, &mut pos, b'{')?;
    consume_key(body, &mut pos, b"\"amount\"")?;
    let amount = read_number(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"installments\"")?;
    let installments = read_number(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"requested_at\"")?;
    let requested_at = read_string(body, &mut pos)?;
    consume_byte(body, &mut pos, b'}')?;
    consume_comma(body, &mut pos)?;

    consume_key(body, &mut pos, b"\"customer\"")?;
    consume_byte(body, &mut pos, b'{')?;
    consume_key(body, &mut pos, b"\"avg_amount\"")?;
    let customer_avg_amount = read_number(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"tx_count_24h\"")?;
    let tx_count_24h = read_number(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"known_merchants\"")?;
    let known_merchants = read_array(body, &mut pos)?;
    consume_byte(body, &mut pos, b'}')?;
    consume_comma(body, &mut pos)?;

    consume_key(body, &mut pos, b"\"merchant\"")?;
    consume_byte(body, &mut pos, b'{')?;
    consume_key(body, &mut pos, b"\"id\"")?;
    let merchant_id = read_string(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"mcc\"")?;
    let mcc = read_string(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"avg_amount\"")?;
    let merchant_avg_amount = read_number(body, &mut pos)?;
    consume_byte(body, &mut pos, b'}')?;
    consume_comma(body, &mut pos)?;

    consume_key(body, &mut pos, b"\"terminal\"")?;
    consume_byte(body, &mut pos, b'{')?;
    consume_key(body, &mut pos, b"\"is_online\"")?;
    let is_online = read_bool(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"card_present\"")?;
    let card_present = read_bool(body, &mut pos)?;
    consume_comma(body, &mut pos)?;
    consume_key(body, &mut pos, b"\"km_from_home\"")?;
    let km_from_home = read_number(body, &mut pos)?;
    consume_byte(body, &mut pos, b'}')?;
    consume_comma(body, &mut pos)?;

    consume_key(body, &mut pos, b"\"last_transaction\"")?;
    let pos_after_last_key = skip_ws(body, pos);
    let (last_timestamp, last_km_from_current) = if body[pos_after_last_key..].starts_with(b"null")
    {
        pos = pos_after_last_key + 4;
        (None, None)
    } else {
        consume_byte(body, &mut pos, b'{')?;
        consume_key(body, &mut pos, b"\"timestamp\"")?;
        let timestamp = read_string(body, &mut pos)?;
        consume_comma(body, &mut pos)?;
        consume_key(body, &mut pos, b"\"km_from_current\"")?;
        let km = read_number(body, &mut pos)?;
        consume_byte(body, &mut pos, b'}')?;
        (Some(timestamp), Some(km))
    };

    consume_byte(body, &mut pos, b'}')?;
    if skip_ws(body, pos) != body.len() {
        return None;
    }

    Some(Payload {
        amount,
        installments,
        requested_at,
        customer_avg_amount,
        tx_count_24h,
        known_merchants,
        merchant_id,
        mcc,
        merchant_avg_amount,
        is_online,
        card_present,
        km_from_home,
        last_timestamp,
        last_km_from_current,
    })
}

fn parse_payload_scan(body: &[u8]) -> Result<Payload<'_>, &'static str> {
    let transaction = object_slice(body, b"\"transaction\"").ok_or("missing transaction")?;
    let customer = object_slice(body, b"\"customer\"").ok_or("missing customer")?;
    let merchant = object_slice(body, b"\"merchant\"").ok_or("missing merchant")?;
    let terminal = object_slice(body, b"\"terminal\"").ok_or("missing terminal")?;

    let last_pos = after_colon(body, b"\"last_transaction\"").ok_or("missing last_transaction")?;
    let last_pos = skip_ws(body, last_pos);
    let (last_timestamp, last_km_from_current) = if body[last_pos..].starts_with(b"null") {
        (None, None)
    } else {
        let last = object_slice(body, b"\"last_transaction\"").ok_or("bad last_transaction")?;
        (
            Some(string_field(last, b"\"timestamp\"").ok_or("missing last timestamp")?),
            Some(number_field(last, b"\"km_from_current\"").ok_or("missing last km")?),
        )
    };

    Ok(Payload {
        amount: number_field(transaction, b"\"amount\"").ok_or("missing amount")?,
        installments: number_field(transaction, b"\"installments\"")
            .ok_or("missing installments")?,
        requested_at: string_field(transaction, b"\"requested_at\"")
            .ok_or("missing requested_at")?,
        customer_avg_amount: number_field(customer, b"\"avg_amount\"")
            .ok_or("missing customer avg")?,
        tx_count_24h: number_field(customer, b"\"tx_count_24h\"").ok_or("missing tx_count_24h")?,
        known_merchants: array_slice(customer, b"\"known_merchants\"")
            .ok_or("missing known_merchants")?,
        merchant_id: string_field(merchant, b"\"id\"").ok_or("missing merchant id")?,
        mcc: string_field(merchant, b"\"mcc\"").ok_or("missing mcc")?,
        merchant_avg_amount: number_field(merchant, b"\"avg_amount\"")
            .ok_or("missing merchant avg")?,
        is_online: bool_field(terminal, b"\"is_online\"").ok_or("missing is_online")?,
        card_present: bool_field(terminal, b"\"card_present\"").ok_or("missing card_present")?,
        km_from_home: number_field(terminal, b"\"km_from_home\"").ok_or("missing km_from_home")?,
        last_timestamp,
        last_km_from_current,
    })
}

fn consume_key(s: &[u8], pos: &mut usize, key: &[u8]) -> Option<()> {
    *pos = skip_ws(s, *pos);
    if !s.get(*pos..)?.starts_with(key) {
        return None;
    }
    *pos += key.len();
    *pos = skip_ws(s, *pos);
    if s.get(*pos) != Some(&b':') {
        return None;
    }
    *pos += 1;
    Some(())
}

fn consume_byte(s: &[u8], pos: &mut usize, expected: u8) -> Option<()> {
    *pos = skip_ws(s, *pos);
    if s.get(*pos) != Some(&expected) {
        return None;
    }
    *pos += 1;
    Some(())
}

fn consume_comma(s: &[u8], pos: &mut usize) -> Option<()> {
    consume_byte(s, pos, b',')
}

fn read_number(s: &[u8], pos: &mut usize) -> Option<f64> {
    *pos = skip_ws(s, *pos);
    let start = *pos;
    while *pos < s.len() {
        let b = s[*pos];
        if b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E') {
            *pos += 1;
        } else {
            break;
        }
    }
    parse_number(&s[start..*pos])
}

fn read_string<'a>(s: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    *pos = skip_ws(s, *pos);
    if s.get(*pos) != Some(&b'"') {
        return None;
    }
    *pos += 1;
    let start = *pos;
    let mut escaped = false;
    while *pos < s.len() {
        let b = s[*pos];
        if escaped {
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        } else if b == b'"' {
            let out = &s[start..*pos];
            *pos += 1;
            return Some(out);
        }
        *pos += 1;
    }
    None
}

fn skip_json_string(s: &[u8], pos: &mut usize) -> Option<()> {
    read_string(s, pos).map(|_| ())
}

fn read_bool(s: &[u8], pos: &mut usize) -> Option<bool> {
    *pos = skip_ws(s, *pos);
    if s.get(*pos..)?.starts_with(b"true") {
        *pos += 4;
        Some(true)
    } else if s.get(*pos..)?.starts_with(b"false") {
        *pos += 5;
        Some(false)
    } else {
        None
    }
}

fn read_array<'a>(s: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    *pos = skip_ws(s, *pos);
    if s.get(*pos) != Some(&b'[') {
        return None;
    }

    let start = *pos;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    while *pos < s.len() {
        let b = s[*pos];
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
        } else if b == b'"' {
            in_string = true;
        } else if b == b'[' {
            depth += 1;
        } else if b == b']' {
            depth -= 1;
            if depth == 0 {
                *pos += 1;
                return Some(&s[start..*pos]);
            }
        }
        *pos += 1;
    }
    None
}

fn object_slice<'a>(s: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    if s.get(pos) != Some(&b'{') {
        return None;
    }
    let start = pos;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    while pos < s.len() {
        let b = s[pos];
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
        } else if b == b'"' {
            in_string = true;
        } else if b == b'{' {
            depth += 1;
        } else if b == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some(&s[start..=pos]);
            }
        }
        pos += 1;
    }
    None
}

fn array_slice<'a>(s: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    if s.get(pos) != Some(&b'[') {
        return None;
    }
    let start = pos;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    while pos < s.len() {
        let b = s[pos];
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
        } else if b == b'"' {
            in_string = true;
        } else if b == b'[' {
            depth += 1;
        } else if b == b']' {
            depth -= 1;
            if depth == 0 {
                return Some(&s[start..=pos]);
            }
        }
        pos += 1;
    }
    None
}

fn number_field(s: &[u8], key: &[u8]) -> Option<f64> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    let start = pos;
    while pos < s.len() {
        let b = s[pos];
        if b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E') {
            pos += 1;
        } else {
            break;
        }
    }
    parse_number(&s[start..pos])
}

fn parse_number(bytes: &[u8]) -> Option<f64> {
    if bytes.is_empty() {
        return None;
    }

    let mut pos = 0usize;
    let mut negative = false;
    if bytes[pos] == b'-' {
        negative = true;
        pos += 1;
    } else if bytes[pos] == b'+' {
        pos += 1;
    }

    let mut value = 0.0f64;
    let mut has_digit = false;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        value = value * 10.0 + f64::from(bytes[pos] - b'0');
        pos += 1;
        has_digit = true;
    }

    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        let mut scale = 0.1f64;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            value += f64::from(bytes[pos] - b'0') * scale;
            scale *= 0.1;
            pos += 1;
            has_digit = true;
        }
    }

    if !has_digit {
        return None;
    }

    if pos < bytes.len() && matches!(bytes[pos], b'e' | b'E') {
        pos += 1;
        let mut exp_negative = false;
        if pos < bytes.len() && bytes[pos] == b'-' {
            exp_negative = true;
            pos += 1;
        } else if pos < bytes.len() && bytes[pos] == b'+' {
            pos += 1;
        }

        let mut exp = 0i32;
        let mut has_exp_digit = false;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            exp = exp
                .saturating_mul(10)
                .saturating_add(i32::from(bytes[pos] - b'0'));
            pos += 1;
            has_exp_digit = true;
        }
        if !has_exp_digit {
            return None;
        }
        value *= 10f64.powi(if exp_negative { -exp } else { exp });
    }

    if pos != bytes.len() {
        return None;
    }
    Some(if negative { -value } else { value })
}

fn string_field<'a>(s: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    if s.get(pos) != Some(&b'"') {
        return None;
    }
    pos += 1;
    let start = pos;
    let mut escaped = false;
    while pos < s.len() {
        let b = s[pos];
        if escaped {
            escaped = false;
        } else if b == b'\\' {
            escaped = true;
        } else if b == b'"' {
            return Some(&s[start..pos]);
        }
        pos += 1;
    }
    None
}

fn bool_field(s: &[u8], key: &[u8]) -> Option<bool> {
    let pos = skip_ws(s, after_colon(s, key)?);
    if s[pos..].starts_with(b"true") {
        Some(true)
    } else if s[pos..].starts_with(b"false") {
        Some(false)
    } else {
        None
    }
}

fn after_colon(s: &[u8], key: &[u8]) -> Option<usize> {
    let key_pos = find_bytes(s, key)?;
    let mut pos = key_pos + key.len();
    while pos < s.len() {
        let b = s[pos];
        if b == b':' {
            return Some(pos + 1);
        }
        if !b.is_ascii_whitespace() {
            return None;
        }
        pos += 1;
    }
    None
}

fn skip_ws(s: &[u8], mut pos: usize) -> usize {
    while pos < s.len() && s[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }

    let first = needle[0];
    let last_start = haystack.len() - needle.len();
    let mut pos = 0usize;
    while pos <= last_start {
        if haystack[pos] == first && &haystack[pos..pos + needle.len()] == needle {
            return Some(pos);
        }
        pos += 1;
    }
    None
}
