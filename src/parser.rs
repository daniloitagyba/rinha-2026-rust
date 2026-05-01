pub struct Payload<'a> {
    pub amount: f64,
    pub installments: f64,
    pub requested_at: &'a str,
    pub customer_avg_amount: f64,
    pub tx_count_24h: f64,
    pub known_merchants: &'a str,
    pub merchant_id: &'a str,
    pub mcc: &'a str,
    pub merchant_avg_amount: f64,
    pub is_online: bool,
    pub card_present: bool,
    pub km_from_home: f64,
    pub last_timestamp: Option<&'a str>,
    pub last_km_from_current: Option<f64>,
}

pub fn parse_payload(body: &str) -> Result<Payload<'_>, &'static str> {
    let transaction = object_slice(body, "\"transaction\"").ok_or("missing transaction")?;
    let customer = object_slice(body, "\"customer\"").ok_or("missing customer")?;
    let merchant = object_slice(body, "\"merchant\"").ok_or("missing merchant")?;
    let terminal = object_slice(body, "\"terminal\"").ok_or("missing terminal")?;

    let last_pos = after_colon(body, "\"last_transaction\"").ok_or("missing last_transaction")?;
    let last_pos = skip_ws(body, last_pos);
    let (last_timestamp, last_km_from_current) = if body[last_pos..].starts_with("null") {
        (None, None)
    } else {
        let last = object_slice(body, "\"last_transaction\"").ok_or("bad last_transaction")?;
        (
            Some(string_field(last, "\"timestamp\"").ok_or("missing last timestamp")?),
            Some(number_field(last, "\"km_from_current\"").ok_or("missing last km")?),
        )
    };

    Ok(Payload {
        amount: number_field(transaction, "\"amount\"").ok_or("missing amount")?,
        installments: number_field(transaction, "\"installments\"")
            .ok_or("missing installments")?,
        requested_at: string_field(transaction, "\"requested_at\"")
            .ok_or("missing requested_at")?,
        customer_avg_amount: number_field(customer, "\"avg_amount\"")
            .ok_or("missing customer avg")?,
        tx_count_24h: number_field(customer, "\"tx_count_24h\"").ok_or("missing tx_count_24h")?,
        known_merchants: array_slice(customer, "\"known_merchants\"")
            .ok_or("missing known_merchants")?,
        merchant_id: string_field(merchant, "\"id\"").ok_or("missing merchant id")?,
        mcc: string_field(merchant, "\"mcc\"").ok_or("missing mcc")?,
        merchant_avg_amount: number_field(merchant, "\"avg_amount\"")
            .ok_or("missing merchant avg")?,
        is_online: bool_field(terminal, "\"is_online\"").ok_or("missing is_online")?,
        card_present: bool_field(terminal, "\"card_present\"").ok_or("missing card_present")?,
        km_from_home: number_field(terminal, "\"km_from_home\"").ok_or("missing km_from_home")?,
        last_timestamp,
        last_km_from_current,
    })
}

fn object_slice<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    if s.as_bytes().get(pos) != Some(&b'{') {
        return None;
    }
    let start = pos;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    while pos < s.len() {
        let b = s.as_bytes()[pos];
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

fn array_slice<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    if s.as_bytes().get(pos) != Some(&b'[') {
        return None;
    }
    let start = pos;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    while pos < s.len() {
        let b = s.as_bytes()[pos];
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

fn number_field(s: &str, key: &str) -> Option<f64> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    let start = pos;
    while pos < s.len() {
        let b = s.as_bytes()[pos];
        if b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E') {
            pos += 1;
        } else {
            break;
        }
    }
    s[start..pos].parse::<f64>().ok()
}

fn string_field<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let mut pos = skip_ws(s, after_colon(s, key)?);
    if s.as_bytes().get(pos) != Some(&b'"') {
        return None;
    }
    pos += 1;
    let start = pos;
    let mut escaped = false;
    while pos < s.len() {
        let b = s.as_bytes()[pos];
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

fn bool_field(s: &str, key: &str) -> Option<bool> {
    let pos = skip_ws(s, after_colon(s, key)?);
    if s[pos..].starts_with("true") {
        Some(true)
    } else if s[pos..].starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn after_colon(s: &str, key: &str) -> Option<usize> {
    let key_pos = s.find(key)?;
    let mut pos = key_pos + key.len();
    while pos < s.len() {
        let b = s.as_bytes()[pos];
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

fn skip_ws(s: &str, mut pos: usize) -> usize {
    while pos < s.len() && s.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}
