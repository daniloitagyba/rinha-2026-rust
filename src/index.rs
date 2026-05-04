use crate::vector::{
    bucket16, bucket4, bucket8, neighbor_keys, QuantizedVector, BUCKET_COUNT, DIM, K,
};
use std::env;
use std::fs::File;
use std::io;
use std::path::Path;

const MAGIC: &[u8; 8] = b"RINHA26I";
const HEADER_LEN: usize = 80;
const PROFILE_KEY_COUNT: usize = 1 << 22;
const RISKY_GROUP_COUNT: usize = 1 << 4;
const LEGIT_MASK: u8 = 1;
const FRAUD_MASK: u8 = 2;

const EXACT_FALLBACK_OFF: u8 = 0;
const EXACT_FALLBACK_UNCERTAIN: u8 = 1;
const EXACT_FALLBACK_RISKY: u8 = 2;
const EXACT_FALLBACK_PROFILE_MISS: u8 = 3;

#[derive(Clone, Copy)]
pub struct SearchParams {
    pub early_candidates: usize,
    pub min_candidates: usize,
    pub max_candidates: usize,
    pub flat: bool,
    pub fast_path: bool,
    pub fast_only: bool,
    pub profile_fast_path: bool,
    pub profile_min_count: usize,
    pub exact_fallback: u8,
    pub overload_min_candidates: usize,
    pub overload_max_candidates: usize,
    pub overload_threshold: usize,
    pub overload_fast_only: bool,
    pub search_fallback_last_distance: i16,
}

impl SearchParams {
    pub fn from_env() -> Self {
        let min_candidates = env_usize("MIN_CANDIDATES", 16_200).max(K);
        let max_candidates = env_usize("MAX_CANDIDATES", 32_400).max(min_candidates);
        let early_candidates = env_usize("EARLY_CANDIDATES", min_candidates)
            .max(K)
            .min(min_candidates);
        let overload_min_candidates = env_usize("OVERLOAD_MIN_CANDIDATES", 3_000);
        let overload_max_candidates =
            env_usize("OVERLOAD_MAX_CANDIDATES", 15_000).max(overload_min_candidates);
        let search_fallback_last_distance =
            env_usize("SEARCH_FALLBACK_LAST_DISTANCE", 2_900).min(i16::MAX as usize) as i16;

        Self {
            early_candidates,
            min_candidates,
            max_candidates,
            flat: env::var("SEARCH_MODE")
                .map(|v| v == "flat")
                .unwrap_or(false),
            fast_path: env_bool("FAST_PATH", false),
            fast_only: env_bool("FAST_ONLY", false),
            profile_fast_path: env_bool("PROFILE_FASTPATH", true),
            profile_min_count: env_usize("PROFILE_MIN_COUNT", 20).max(1),
            exact_fallback: exact_fallback_mode(env::var("EXACT_FALLBACK").ok().as_deref()),
            overload_min_candidates,
            overload_max_candidates,
            overload_threshold: env_usize("OVERLOAD_THRESHOLD", 0),
            overload_fast_only: env_bool("OVERLOAD_FAST_ONLY", true),
            search_fallback_last_distance,
        }
    }

    pub fn for_load(&self, load: usize) -> Self {
        if self.overload_threshold == 0 || load < self.overload_threshold || self.flat {
            return *self;
        }

        let mut params = *self;
        params.early_candidates = self.overload_min_candidates.min(self.early_candidates);
        params.min_candidates = self.overload_min_candidates.min(self.min_candidates);
        params.max_candidates = self.overload_max_candidates.min(self.max_candidates);
        if params.early_candidates > params.min_candidates {
            params.early_candidates = params.min_candidates;
        }
        params.early_candidates = params.early_candidates.max(K);
        if params.max_candidates < params.min_candidates {
            params.max_candidates = params.min_candidates;
        }
        params.fast_only = self.overload_fast_only;
        params
    }
}

pub struct Index {
    mmap: Mmap,
    count: usize,
    vectors_offset: usize,
    labels_offset: usize,
    bucket_offsets_offset: usize,
    bucket_items_offset: usize,
    profile_counts: Vec<u16>,
    profile_label_masks: Vec<u8>,
    risky_fallback_ids: Vec<u32>,
    risky_fallback_groups: Vec<Vec<u32>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionKind {
    ProfileFast,
    RuleFast,
    Approx,
    ExactFlat,
    ExactRisky,
}

impl DecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProfileFast => "profile_fast",
            Self::RuleFast => "rule_fast",
            Self::Approx => "approx",
            Self::ExactFlat => "exact_flat",
            Self::ExactRisky => "exact_risky",
        }
    }
}

impl Index {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let file = File::open(path.as_ref())
            .map_err(|e| format!("failed to open index {}: {e}", path.as_ref().display()))?;
        let mmap = Mmap::map(&file).map_err(|e| format!("failed to mmap index: {e}"))?;
        let bytes = mmap.as_slice();
        if bytes.len() < HEADER_LEN {
            return Err("index too small".to_string());
        }
        if bytes[0..8] != MAGIC[..] {
            return Err("bad index magic".to_string());
        }
        let version = read_u32(bytes, 8)?;
        let dim = read_u32(bytes, 12)?;
        let count = read_u32(bytes, 16)? as usize;
        let bucket_count = read_u32(bytes, 24)? as usize;
        let vectors_offset = read_u64(bytes, 32)? as usize;
        let labels_offset = read_u64(bytes, 40)? as usize;
        let bucket_offsets_offset = read_u64(bytes, 48)? as usize;
        let bucket_items_offset = read_u64(bytes, 56)? as usize;
        let file_len = read_u64(bytes, 64)? as usize;

        if version != 1 || dim != DIM as u32 || bucket_count != BUCKET_COUNT {
            return Err("unsupported index version or shape".to_string());
        }
        if file_len != bytes.len() {
            return Err("index file length mismatch".to_string());
        }
        let vectors_end = vectors_offset + count * DIM * 2;
        let labels_end = labels_offset + count;
        let bucket_offsets_end = bucket_offsets_offset + (BUCKET_COUNT + 1) * 4;
        let bucket_items_end = bucket_items_offset + count * 4;
        if vectors_end > bytes.len()
            || labels_end > bytes.len()
            || bucket_offsets_end > bytes.len()
            || bucket_items_end > bytes.len()
        {
            return Err("index offsets out of bounds".to_string());
        }
        let (profile_counts, profile_label_masks) =
            build_profile_stats(bytes, count, vectors_offset, labels_offset);
        let risky_fallback_filter = RiskyFallbackFilter::from_env();
        let (risky_fallback_ids, risky_fallback_groups) =
            build_risky_fallback_index(bytes, count, vectors_offset, &risky_fallback_filter);

        Ok(Self {
            mmap,
            count,
            vectors_offset,
            labels_offset,
            bucket_offsets_offset,
            bucket_items_offset,
            profile_counts,
            profile_label_masks,
            risky_fallback_ids,
            risky_fallback_groups,
        })
    }

    pub fn classify(&self, query: &QuantizedVector, params: &SearchParams) -> (bool, f32) {
        let (approved, score, _) = self.classify_detailed(query, params);
        (approved, score)
    }

    pub fn risky_fallback_count(&self) -> usize {
        self.risky_fallback_ids.len()
    }

    pub fn prefault(&self) -> usize {
        let bytes = self.mmap.as_slice();
        let mut checksum = 0usize;
        let mut pos = 0usize;
        while pos < bytes.len() {
            checksum ^= unsafe { std::ptr::read_volatile(bytes.as_ptr().add(pos)) as usize };
            pos += 4096;
        }
        if !bytes.is_empty() {
            checksum ^=
                unsafe { std::ptr::read_volatile(bytes.as_ptr().add(bytes.len() - 1)) as usize };
        }
        checksum
    }

    pub fn classify_detailed(
        &self,
        query: &QuantizedVector,
        params: &SearchParams,
    ) -> (bool, f32, DecisionKind) {
        if let Some(frauds) = self.try_profile_fast_decision(query, params) {
            return decision_from_frauds(frauds, DecisionKind::ProfileFast);
        }

        if params.fast_path || params.fast_only {
            if let Some(result) = fast_classify(query) {
                let frauds = if result.0 { 0 } else { K };
                return decision_from_frauds(frauds, DecisionKind::RuleFast);
            }
        }
        if params.fast_only
            && !selective_search_fallback(query, params.search_fallback_last_distance)
        {
            return decision_from_frauds(K, DecisionKind::RuleFast);
        }

        if params.flat || params.exact_fallback == EXACT_FALLBACK_PROFILE_MISS {
            let frauds = self.classify_flat(query);
            return decision_from_frauds(frauds, DecisionKind::ExactFlat);
        }

        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];

        let mut keys = [0u16; BUCKET_COUNT];
        let key_count = neighbor_keys(query, &mut keys);
        let mut candidates = 0usize;

        for key in keys.iter().take(key_count) {
            let start = self.bucket_offset(*key as usize);
            let end = self.bucket_offset(*key as usize + 1);

            for item_pos in start..end {
                let id = self.bucket_item(item_pos);
                self.consider(id, query, &mut top_dist, &mut top_label);
                candidates += 1;
                if candidates >= params.max_candidates {
                    break;
                }
            }

            if candidates >= params.max_candidates
                || candidates >= params.min_candidates
                || (candidates >= params.early_candidates
                    && top_dist[K - 1] != i64::MAX
                    && strong_decision(&top_label))
            {
                break;
            }
        }

        if candidates < K {
            let frauds = self.classify_flat(query);
            return decision_from_frauds(frauds, DecisionKind::ExactFlat);
        }

        let frauds = count_frauds(&top_label);
        if !should_use_exact_fallback(query, frauds, params) {
            return decision_from_frauds(frauds, DecisionKind::Approx);
        }

        if params.exact_fallback == EXACT_FALLBACK_RISKY {
            let frauds = self.classify_risky_flat(query, true);
            decision_from_frauds(frauds, DecisionKind::ExactRisky)
        } else {
            let frauds = self.classify_flat(query);
            decision_from_frauds(frauds, DecisionKind::ExactFlat)
        }
    }

    fn classify_flat(&self, query: &QuantizedVector) -> usize {
        self.classify_all_ids(query)
    }

    fn classify_all_ids(&self, query: &QuantizedVector) -> usize {
        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];

        for id in 0..self.count {
            self.consider(id as u32, query, &mut top_dist, &mut top_label);
        }

        count_frauds(&top_label)
    }

    fn classify_ids(&self, query: &QuantizedVector, ids: &[u32]) -> usize {
        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];

        for &id in ids {
            self.consider(id, query, &mut top_dist, &mut top_label);
        }

        count_frauds(&top_label)
    }

    fn classify_risky_flat(&self, query: &QuantizedVector, allow_full_tiebreak: bool) -> usize {
        if self.risky_fallback_ids.len() < K {
            return self.classify_flat(query);
        }

        let group_key = risky_group_key(query);
        let candidates = self
            .risky_fallback_groups
            .get(group_key)
            .filter(|ids| ids.len() >= K)
            .map(Vec::as_slice)
            .unwrap_or(&self.risky_fallback_ids);
        let frauds = self.classify_ids(query, candidates);
        if allow_full_tiebreak && needs_full_risky_tiebreak(query, frauds) {
            self.classify_flat(query)
        } else {
            frauds
        }
    }

    fn try_profile_fast_decision(
        &self,
        query: &QuantizedVector,
        params: &SearchParams,
    ) -> Option<usize> {
        if !params.profile_fast_path {
            return None;
        }

        let key = profile_key(query);
        if (self.profile_counts[key] as usize) < params.profile_min_count {
            return None;
        }

        match self.profile_label_masks[key] {
            LEGIT_MASK => Some(0),
            FRAUD_MASK => Some(K),
            _ => None,
        }
    }

    #[inline(always)]
    fn consider(
        &self,
        id: u32,
        query: &QuantizedVector,
        top_dist: &mut [i64; K],
        top_label: &mut [u8; K],
    ) {
        let dist = self.distance_sq(id as usize, query, top_dist[K - 1]);
        if dist >= top_dist[K - 1] {
            return;
        }

        let label = self.label(id as usize);
        if dist < top_dist[0] {
            top_dist[4] = top_dist[3];
            top_dist[3] = top_dist[2];
            top_dist[2] = top_dist[1];
            top_dist[1] = top_dist[0];
            top_dist[0] = dist;
            top_label[4] = top_label[3];
            top_label[3] = top_label[2];
            top_label[2] = top_label[1];
            top_label[1] = top_label[0];
            top_label[0] = label;
        } else if dist < top_dist[1] {
            top_dist[4] = top_dist[3];
            top_dist[3] = top_dist[2];
            top_dist[2] = top_dist[1];
            top_dist[1] = dist;
            top_label[4] = top_label[3];
            top_label[3] = top_label[2];
            top_label[2] = top_label[1];
            top_label[1] = label;
        } else if dist < top_dist[2] {
            top_dist[4] = top_dist[3];
            top_dist[3] = top_dist[2];
            top_dist[2] = dist;
            top_label[4] = top_label[3];
            top_label[3] = top_label[2];
            top_label[2] = label;
        } else if dist < top_dist[3] {
            top_dist[4] = top_dist[3];
            top_dist[3] = dist;
            top_label[4] = top_label[3];
            top_label[3] = label;
        } else {
            top_dist[4] = dist;
            top_label[4] = label;
        }
    }

    #[inline(always)]
    fn distance_sq(&self, id: usize, query: &QuantizedVector, cutoff: i64) -> i64 {
        let start = self.vectors_offset + id * DIM * 2;
        let bytes = self.mmap.as_slice();
        let mut sum = 0i64;
        add_dim(bytes, start, query, 6, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 10, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 9, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 5, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 11, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 2, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 4, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 7, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 0, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 1, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 8, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 12, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 3, &mut sum);
        if sum >= cutoff {
            return sum;
        }
        add_dim(bytes, start, query, 13, &mut sum);
        sum
    }

    #[inline(always)]
    fn label(&self, id: usize) -> u8 {
        self.mmap.as_slice()[self.labels_offset + id]
    }

    #[inline(always)]
    fn bucket_offset(&self, key: usize) -> usize {
        let pos = self.bucket_offsets_offset + key * 4;
        read_u32_unchecked(self.mmap.as_slice(), pos) as usize
    }

    #[inline(always)]
    fn bucket_item(&self, pos: usize) -> u32 {
        let byte_pos = self.bucket_items_offset + pos * 4;
        read_u32_unchecked(self.mmap.as_slice(), byte_pos)
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn exact_fallback_mode(value: Option<&str>) -> u8 {
    match value {
        Some("1" | "uncertain" | "UNCERTAIN") => EXACT_FALLBACK_UNCERTAIN,
        Some("2" | "risky" | "RISKY") => EXACT_FALLBACK_RISKY,
        Some("3" | "profile" | "PROFILE" | "profile_miss" | "PROFILE_MISS") => {
            EXACT_FALLBACK_PROFILE_MISS
        }
        _ => EXACT_FALLBACK_OFF,
    }
}

pub fn exact_fallback_name(mode: u8) -> &'static str {
    match mode {
        EXACT_FALLBACK_UNCERTAIN => "uncertain",
        EXACT_FALLBACK_RISKY => "risky",
        EXACT_FALLBACK_PROFILE_MISS => "profile_miss",
        _ => "off",
    }
}

fn decision_from_frauds(frauds: usize, kind: DecisionKind) -> (bool, f32, DecisionKind) {
    let score = frauds as f32 / K as f32;
    (frauds < 3, score, kind)
}

fn count_frauds(top_label: &[u8; K]) -> usize {
    top_label.iter().filter(|&&label| label == 1).count()
}

fn strong_decision(top_label: &[u8; K]) -> bool {
    let frauds = count_frauds(top_label);
    frauds <= 1 || frauds >= 4
}

fn should_use_exact_fallback(
    query: &QuantizedVector,
    frauds: usize,
    params: &SearchParams,
) -> bool {
    if frauds > 0 && frauds < K {
        return matches!(
            params.exact_fallback,
            EXACT_FALLBACK_UNCERTAIN | EXACT_FALLBACK_RISKY
        );
    }

    params.exact_fallback == EXACT_FALLBACK_RISKY && is_strong_fallback_risk(query, frauds)
}

fn is_strong_fallback_risk(query: &QuantizedVector, frauds: usize) -> bool {
    if frauds != 0 && frauds != K {
        return false;
    }

    if frauds == 0 && is_high_risk_online_fallback(query) {
        return true;
    }

    if frauds == 0 && is_no_last_moderate_risk_fallback(query) {
        return true;
    }

    query[5] >= 0
        && query[10] == 0
        && query[0] >= 450
        && query[0] <= 1_100
        && query[2] >= 900
        && query[2] <= 2_500
        && query[7] >= 500
        && query[7] <= 2_000
        && query[8] >= 2_000
        && query[8] <= 4_500
}

fn is_no_last_moderate_risk_fallback(query: &QuantizedVector) -> bool {
    query[5] < 0
        && query[11] == 0
        && query[0] >= 350
        && query[0] <= 700
        && query[1] >= 3_000
        && query[1] <= 6_500
        && query[2] >= 900
        && query[2] <= 2_200
        && query[7] >= 350
        && query[7] <= 1_000
        && query[8] >= 2_000
        && query[8] <= 3_500
        && query[12] <= 5_000
        && query[13] <= 300
}

fn needs_full_risky_tiebreak(query: &QuantizedVector, frauds: usize) -> bool {
    if query[5] < 0 || query[9] <= 0 || query[10] != 0 {
        return false;
    }

    if frauds >= 3 {
        return query[11] == 0
            && query[12] <= 1_700
            && query[0] >= 500
            && query[0] <= 900
            && query[2] >= 1_000
            && query[2] <= 2_200
            && query[7] >= 350
            && query[7] <= 900
            && query[8] >= 1_800
            && query[8] <= 3_000;
    }

    is_high_risk_online_fallback(query)
}

fn is_high_risk_online_fallback(query: &QuantizedVector) -> bool {
    query[12] >= 8_000
        && query[1] >= 5_500
        && query[6] >= 1_000
        && query[6] <= 1_700
        && query[7] >= 300
        && query[7] <= 4_200
        && query[8] >= 3_800
        && query[8] <= 6_000
        && ((query[0] >= 450 && query[0] <= 600 && query[2] <= 1_200)
            || (query[0] >= 2_500 && query[0] <= 3_100 && query[2] >= 9_000))
}

#[inline(always)]
fn add_dim(bytes: &[u8], vector_start: usize, query: &QuantizedVector, dim: usize, sum: &mut i64) {
    let candidate = read_i16_unchecked(bytes, vector_start + dim * 2) as i64;
    let d = query[dim] as i64 - candidate;
    *sum += d * d;
}

#[inline(always)]
fn read_i16_unchecked(bytes: &[u8], pos: usize) -> i16 {
    debug_assert!(pos + 2 <= bytes.len());
    unsafe { i16::from_le(std::ptr::read_unaligned(bytes.as_ptr().add(pos) as *const i16)) }
}

fn build_profile_stats(
    bytes: &[u8],
    count: usize,
    vectors_offset: usize,
    labels_offset: usize,
) -> (Vec<u16>, Vec<u8>) {
    let mut profile_counts = vec![0u16; PROFILE_KEY_COUNT];
    let mut profile_label_masks = vec![0u8; PROFILE_KEY_COUNT];

    for id in 0..count {
        let key = profile_key_at(bytes, vectors_offset + id * DIM * 2);
        profile_counts[key] = profile_counts[key].saturating_add(1);
        let label = bytes[labels_offset + id];
        profile_label_masks[key] |= if label == 1 { FRAUD_MASK } else { LEGIT_MASK };
    }

    (profile_counts, profile_label_masks)
}

fn build_risky_fallback_index(
    bytes: &[u8],
    count: usize,
    vectors_offset: usize,
    filter: &RiskyFallbackFilter,
) -> (Vec<u32>, Vec<Vec<u32>>) {
    let mut ids = Vec::with_capacity(128_000);
    let mut groups = Vec::with_capacity(RISKY_GROUP_COUNT);
    for _ in 0..RISKY_GROUP_COUNT {
        groups.push(Vec::new());
    }
    for id in 0..count {
        let start = vectors_offset + id * DIM * 2;
        if is_risky_fallback_reference(bytes, start, filter) {
            let item = id as u32;
            ids.push(item);
            groups[risky_group_key_at(bytes, start)].push(item);
        }
    }
    (ids, groups)
}

fn is_risky_fallback_reference(
    bytes: &[u8],
    vector_start: usize,
    filter: &RiskyFallbackFilter,
) -> bool {
    let amount = read_i16_unchecked(bytes, vector_start) as i32;
    if amount < filter.amount_min || amount > filter.amount_max {
        return false;
    }

    let installments = read_i16_unchecked(bytes, vector_start + 2) as i32;
    if installments < filter.installments_min || installments > filter.installments_max {
        return false;
    }

    if (read_i16_unchecked(bytes, vector_start + 4) as i32) < filter.ratio_min {
        return false;
    }

    let km_home = read_i16_unchecked(bytes, vector_start + 14) as i32;
    if km_home < filter.km_home_min || km_home > filter.km_home_max {
        return false;
    }

    let tx24h = read_i16_unchecked(bytes, vector_start + 16) as i32;
    if tx24h < filter.tx24h_min || tx24h > filter.tx24h_max {
        return false;
    }

    let merchant_average = read_i16_unchecked(bytes, vector_start + 26) as i32;
    merchant_average >= filter.merchant_avg_min && merchant_average <= filter.merchant_avg_max
}

fn profile_key(vector: &QuantizedVector) -> usize {
    let mut key = 0usize;
    key |= bucket16(vector[2]) as usize;
    key |= (bucket8(vector[7]) as usize) << 4;
    key |= (bucket4(vector[8]) as usize) << 7;
    key |= (bucket4(vector[12]) as usize) << 9;
    key |= (bucket4(vector[0]) as usize) << 11;
    key |= (if vector[5] < 0 { 1 } else { 0 }) << 13;
    key |= (if vector[9] > 0 { 1 } else { 0 }) << 14;
    key |= (if vector[10] > 0 { 1 } else { 0 }) << 15;
    key |= (if vector[11] > 0 { 1 } else { 0 }) << 16;
    key |= (bucket4(vector[6]) as usize) << 17;
    key |= (if vector[1] > 1_000 { 1 } else { 0 }) << 19;
    key |= (bucket4(vector[13]) as usize) << 20;
    key
}

fn profile_key_at(bytes: &[u8], vector_start: usize) -> usize {
    let mut key = 0usize;
    key |= bucket16(read_i16_unchecked(bytes, vector_start + 4)) as usize;
    key |= (bucket8(read_i16_unchecked(bytes, vector_start + 14)) as usize) << 4;
    key |= (bucket4(read_i16_unchecked(bytes, vector_start + 16)) as usize) << 7;
    key |= (bucket4(read_i16_unchecked(bytes, vector_start + 24)) as usize) << 9;
    key |= (bucket4(read_i16_unchecked(bytes, vector_start)) as usize) << 11;
    key |= (if read_i16_unchecked(bytes, vector_start + 10) < 0 {
        1
    } else {
        0
    }) << 13;
    key |= (if read_i16_unchecked(bytes, vector_start + 18) > 0 {
        1
    } else {
        0
    }) << 14;
    key |= (if read_i16_unchecked(bytes, vector_start + 20) > 0 {
        1
    } else {
        0
    }) << 15;
    key |= (if read_i16_unchecked(bytes, vector_start + 22) > 0 {
        1
    } else {
        0
    }) << 16;
    key |= (bucket4(read_i16_unchecked(bytes, vector_start + 12)) as usize) << 17;
    key |= (if read_i16_unchecked(bytes, vector_start + 2) > 1_000 {
        1
    } else {
        0
    }) << 19;
    key |= (bucket4(read_i16_unchecked(bytes, vector_start + 26)) as usize) << 20;
    key
}

fn risky_group_key(query: &QuantizedVector) -> usize {
    let mut key = 0usize;
    key |= if query[5] < 0 { 1 } else { 0 };
    key |= (if query[9] > 0 { 1 } else { 0 }) << 1;
    key |= (if query[10] > 0 { 1 } else { 0 }) << 2;
    key |= (if query[11] > 0 { 1 } else { 0 }) << 3;
    key
}

fn risky_group_key_at(bytes: &[u8], vector_start: usize) -> usize {
    let mut key = 0usize;
    key |= if read_i16_unchecked(bytes, vector_start + 10) < 0 {
        1
    } else {
        0
    };
    key |= (if read_i16_unchecked(bytes, vector_start + 18) > 0 {
        1
    } else {
        0
    }) << 1;
    key |= (if read_i16_unchecked(bytes, vector_start + 20) > 0 {
        1
    } else {
        0
    }) << 2;
    key |= (if read_i16_unchecked(bytes, vector_start + 22) > 0 {
        1
    } else {
        0
    }) << 3;
    key
}

struct RiskyFallbackFilter {
    amount_min: i32,
    amount_max: i32,
    installments_min: i32,
    installments_max: i32,
    ratio_min: i32,
    km_home_min: i32,
    km_home_max: i32,
    tx24h_min: i32,
    tx24h_max: i32,
    merchant_avg_min: i32,
    merchant_avg_max: i32,
}

impl RiskyFallbackFilter {
    fn from_env() -> Self {
        Self {
            amount_min: env_usize("RISKY_AMOUNT_MIN", 350) as i32,
            amount_max: env_usize("RISKY_AMOUNT_MAX", 3_200) as i32,
            installments_min: env_usize("RISKY_INSTALLMENTS_MIN", 2_000) as i32,
            installments_max: env_usize("RISKY_INSTALLMENTS_MAX", 6_500) as i32,
            ratio_min: env_usize("RISKY_RATIO_MIN", 750) as i32,
            km_home_min: env_usize("RISKY_KM_HOME_MIN", 200) as i32,
            km_home_max: env_usize("RISKY_KM_HOME_MAX", 4_300) as i32,
            tx24h_min: env_usize("RISKY_TX24H_MIN", 1_500) as i32,
            tx24h_max: env_usize("RISKY_TX24H_MAX", 6_000) as i32,
            merchant_avg_min: env_usize("RISKY_MERCHANT_AVG_MIN", 0) as i32,
            merchant_avg_max: env_usize("RISKY_MERCHANT_AVG_MAX", 450) as i32,
        }
    }
}

fn fast_classify(v: &QuantizedVector) -> Option<(bool, f32)> {
    let no_last = v[5] < 0;
    let last_looks_legit = no_last || (v[5] >= 200 && v[6] <= 350);
    let last_looks_fraud = no_last || (v[5] <= 80 && v[6] >= 1_800);

    if v[0] <= 600
        && v[1] <= 3_400
        && v[2] <= 700
        && (3_400..=8_800).contains(&v[3])
        && v[7] <= 650
        && v[8] <= 3_000
        && v[11] == 0
        && v[12] <= 4_500
        && last_looks_legit
    {
        return Some((true, 0.0));
    }

    if v[0] >= 1_900
        && v[1] >= 5_000
        && v[2] >= 6_500
        && v[3] <= 2_700
        && v[7] >= 1_800
        && v[8] >= 4_000
        && v[11] == 10_000
        && v[12] >= 7_500
        && last_looks_fraud
    {
        return Some((false, 1.0));
    }

    if likely_fraud_shape(v) && !uncertain_fraud_shape(v) {
        return Some((false, 1.0));
    }

    None
}

fn likely_fraud_shape(v: &QuantizedVector) -> bool {
    (v[5] >= 0 && v[5] <= 120 && v[6] >= 800)
        || (v[5] >= 0 && v[5] <= 200 && v[6] >= 1_200)
        || (v[11] == 10_000 && v[8] >= 3_500 && v[2] >= 3_500)
        || (v[11] == 10_000 && v[8] >= 3_500 && v[7] >= 1_200)
        || (v[11] == 10_000 && v[12] >= 7_500 && v[8] >= 3_000)
        || (v[2] >= 8_000 && v[8] >= 3_500)
        || (v[7] >= 2_500 && v[8] >= 3_500)
        || (v[0] >= 1_500 && v[1] >= 4_167 && v[8] >= 3_500)
        || (v[9] == 10_000 && v[10] == 0 && v[11] == 10_000 && v[8] >= 3_000)
        || (v[10] == 0 && v[8] >= 4_000 && (v[2] >= 5_000 || v[7] >= 2_000))
}

fn uncertain_fraud_shape(v: &QuantizedVector) -> bool {
    v[0] <= 3_000 && v[1] <= 5_833 && v[3] >= 3_000 && v[8] <= 5_500 && v[13] >= 100
}

fn selective_search_fallback(v: &QuantizedVector, last_distance_threshold: i16) -> bool {
    v[6] <= last_distance_threshold
}

fn read_u32(bytes: &[u8], pos: usize) -> Result<u32, String> {
    if pos + 4 > bytes.len() {
        return Err("unexpected eof reading u32".to_string());
    }
    Ok(read_u32_unchecked(bytes, pos))
}

fn read_u64(bytes: &[u8], pos: usize) -> Result<u64, String> {
    if pos + 8 > bytes.len() {
        return Err("unexpected eof reading u64".to_string());
    }
    Ok(u64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ]))
}

#[inline(always)]
fn read_u32_unchecked(bytes: &[u8], pos: usize) -> u32 {
    debug_assert!(pos + 4 <= bytes.len());
    unsafe { u32::from_le(std::ptr::read_unaligned(bytes.as_ptr().add(pos) as *const u32)) }
}

pub struct Mmap {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    fn map(file: &File) -> io::Result<Self> {
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty file"));
        }
        #[cfg(unix)]
        {
            use std::ffi::c_void;
            use std::os::fd::AsRawFd;
            const PROT_READ: i32 = 0x1;
            const MAP_PRIVATE: i32 = 0x02;

            extern "C" {
                fn mmap(
                    addr: *mut c_void,
                    length: usize,
                    prot: i32,
                    flags: i32,
                    fd: i32,
                    offset: isize,
                ) -> *mut c_void;
            }

            let ptr = unsafe {
                mmap(
                    std::ptr::null_mut(),
                    len,
                    PROT_READ,
                    MAP_PRIVATE,
                    file.as_raw_fd(),
                    0,
                )
            };
            if ptr as isize == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(Self {
                ptr: ptr as *mut u8,
                len,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = file;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "mmap is only implemented for unix targets",
            ))
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            use std::ffi::c_void;
            extern "C" {
                fn munmap(addr: *mut c_void, length: usize) -> i32;
            }
            let _ = munmap(self.ptr as *mut c_void, self.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{fast_classify, selective_search_fallback, SearchParams};
    use crate::vector::QuantizedVector;

    #[test]
    fn fast_path_accepts_only_obvious_legit() {
        let vector: QuantizedVector = [
            410, 1667, 500, 7826, 3333, -10000, -10000, 292, 1500, 0, 10000, 0, 1500, 60,
        ];

        assert_eq!(fast_classify(&vector), Some((true, 0.0)));
    }

    #[test]
    fn fast_path_rejects_only_obvious_fraud() {
        let vector: QuantizedVector = [
            9506, 8333, 10000, 2174, 8333, -10000, -10000, 9523, 10000, 0, 10000, 10000, 7500, 55,
        ];

        assert_eq!(fast_classify(&vector), Some((false, 1.0)));
    }

    #[test]
    fn fast_path_ignores_borderline_shape() {
        let vector: QuantizedVector = [
            1_200, 3_333, 3_000, 6_000, 5_000, 500, 400, 1_000, 3_000, 0, 10_000, 0, 5_000, 2_000,
        ];

        assert_eq!(fast_classify(&vector), None);
    }

    #[test]
    fn fast_path_rejects_fraud_shape_without_search() {
        let vector: QuantizedVector = [
            1_600, 4_167, 5_000, 2_000, 5_000, 500, 400, 1_000, 3_500, 0, 10_000, 0, 5_000, 50,
        ];

        assert_eq!(fast_classify(&vector), Some((false, 1.0)));
    }

    #[test]
    fn fast_path_defers_uncertain_fraud_shape_to_search() {
        let vector: QuantizedVector = [
            1_600, 4_167, 5_000, 6_000, 5_000, 500, 400, 1_000, 3_500, 0, 10_000, 0, 5_000, 2_000,
        ];

        assert_eq!(fast_classify(&vector), None);
    }

    #[test]
    fn overload_switches_to_fast_only() {
        let params = SearchParams {
            early_candidates: 10_000,
            min_candidates: 10_000,
            max_candidates: 40_000,
            flat: false,
            fast_path: true,
            fast_only: false,
            profile_fast_path: true,
            profile_min_count: 20,
            exact_fallback: 0,
            overload_min_candidates: 3_000,
            overload_max_candidates: 15_000,
            overload_threshold: 8,
            overload_fast_only: true,
            search_fallback_last_distance: 2_900,
        };

        assert!(!params.for_load(7).fast_only);
        let overloaded = params.for_load(8);
        assert!(overloaded.fast_only);
        assert_eq!(overloaded.early_candidates, 3_000);
        assert_eq!(overloaded.min_candidates, 3_000);
        assert_eq!(overloaded.max_candidates, 15_000);
    }

    #[test]
    fn selective_fallback_uses_last_distance_threshold() {
        let mut vector: QuantizedVector = [0; 14];

        vector[6] = 2_999;
        assert!(selective_search_fallback(&vector, 3_000));

        vector[6] = 3_001;
        assert!(!selective_search_fallback(&vector, 3_000));
    }
}
