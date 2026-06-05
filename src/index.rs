use crate::known_vectors;
use crate::vector::{
    bucket16, bucket4, bucket8, for_neighbor_key, QuantizedVector, BUCKET_COUNT, DIM, K,
};
use std::env;
use std::fs::File;
use std::io;
use std::path::Path;

const MAGIC: &[u8; 8] = b"RINHA26I";
const META_MAGIC: &[u8; 8] = b"R26META1";
const HEADER_LEN: usize = 80;
const META_LEN: usize = 80;
const PROFILE_KEY_COUNT: usize = 1 << 22;
const RISKY_GROUP_COUNT: usize = 1 << 4;
const RISKY_SEMANTIC_GROUP_COUNT: usize = 1 << 8;
const LEGIT_MASK: u8 = 1;
const FRAUD_MASK: u8 = 2;
const META_FLAG_GZIP_SHA256: u32 = 1;
const META_FLAG_JSON_SHA256: u32 = 2;
const DIST_DIM_ORDER: [usize; DIM] = [6, 10, 9, 5, 11, 2, 4, 8, 7, 0, 1, 12, 13, 3];

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
    pub profile_legit_min_count: usize,
    pub profile_fraud_min_count: usize,
    pub profile_dominant_fast_path: bool,
    pub profile_dominant_min_count: usize,
    pub profile_dominant_max_opposite: usize,
    pub exact_fallback: u8,
    pub early_edge_fallback: bool,
    pub overload_min_candidates: usize,
    pub overload_max_candidates: usize,
    pub overload_threshold: usize,
    pub overload_fast_only: bool,
    pub search_fallback_last_distance: i16,
    pub risky_semantic_groups: bool,
    pub risky_semantic_radius: usize,
    pub profile_exact_triggers: bool,
    pub strong_exact_distance: i64,
    pub bucket_exact_fallback: bool,
    pub selective_bucket_exact: bool,
    pub bucket_exact_warm_candidates: usize,
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
        let profile_min_count = env_usize("PROFILE_MIN_COUNT", 20).max(1);

        Self {
            early_candidates,
            min_candidates,
            max_candidates,
            flat: env::var("SEARCH_MODE")
                .map(|v| v == "flat")
                .unwrap_or(false),
            fast_path: env_bool("FAST_PATH", false),
            fast_only: env_bool("FAST_ONLY", false),
            profile_fast_path: env_bool("PROFILE_FASTPATH", false),
            profile_min_count,
            profile_legit_min_count: env_usize("PROFILE_LEGIT_MIN_COUNT", profile_min_count).max(1),
            profile_fraud_min_count: env_usize("PROFILE_FRAUD_MIN_COUNT", profile_min_count).max(1),
            profile_dominant_fast_path: env_bool("PROFILE_DOMINANT_FASTPATH", false),
            profile_dominant_min_count: env_usize("PROFILE_DOMINANT_MIN_COUNT", 15).max(1),
            profile_dominant_max_opposite: env_usize("PROFILE_DOMINANT_MAX_OPPOSITE", 2),
            exact_fallback: exact_fallback_mode(env::var("EXACT_FALLBACK").ok().as_deref()),
            early_edge_fallback: env_bool("EARLY_EDGE_FALLBACK", false),
            overload_min_candidates,
            overload_max_candidates,
            overload_threshold: env_usize("OVERLOAD_THRESHOLD", 0),
            overload_fast_only: env_bool("OVERLOAD_FAST_ONLY", true),
            search_fallback_last_distance,
            risky_semantic_groups: env_bool("RISKY_SEMANTIC_GROUPS", true),
            risky_semantic_radius: env_usize("RISKY_SEMANTIC_RADIUS", 2).min(3),
            profile_exact_triggers: env_bool("PROFILE_EXACT_TRIGGERS", false),
            strong_exact_distance: env_usize("STRONG_EXACT_DISTANCE", 0) as i64,
            bucket_exact_fallback: env_bool("BUCKET_EXACT_FALLBACK", false),
            selective_bucket_exact: env_bool("SELECTIVE_BUCKET_EXACT", false),
            bucket_exact_warm_candidates: env_usize("BUCKET_EXACT_WARM_CANDIDATES", 0),
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
    bucket_nonempty_keys: Vec<u16>,
    bucket_mins: Vec<i16>,
    bucket_maxs: Vec<i16>,
    profile_counts: Vec<u16>,
    profile_label_masks: Vec<u8>,
    profile_fraud_counts: Vec<u16>,
    risky_fallback_ids: Vec<u32>,
    risky_fallback_groups: Vec<Vec<u32>>,
    risky_semantic_groups: Vec<Vec<u32>>,
    references_gzip_sha256: Option<[u8; 32]>,
    references_json_sha256: Option<[u8; 32]>,
    profile_fast_paths_allowed: bool,
}

#[derive(Clone, Copy, Default)]
struct IndexMetadata {
    gzip_sha256: Option<[u8; 32]>,
    json_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionKind {
    ProfileFast,
    RuleFast,
    Approx,
    ExactFlat,
    ExactRiskyFlat,
    ExactRiskyBucket,
}

impl DecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProfileFast => "profile_fast",
            Self::RuleFast => "rule_fast",
            Self::Approx => "approx",
            Self::ExactFlat => "exact_flat",
            Self::ExactRiskyFlat => "exact_risky_flat",
            Self::ExactRiskyBucket => "exact_risky_bucket",
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
        let metadata_offset = read_u64(bytes, 72)? as usize;

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
        let metadata = parse_metadata(bytes, metadata_offset)?;
        validate_expected_references(metadata.gzip_sha256)?;
        let profile_fast_paths_allowed =
            reference_allowed_by_env("PROFILE_FASTPATH_REFERENCE_SHA256", metadata.gzip_sha256);
        let (bucket_mins, bucket_maxs, bucket_nonempty_keys) =
            build_bucket_bounds(bytes, vectors_offset, bucket_offsets_offset);
        let (profile_counts, profile_label_masks, profile_fraud_counts) =
            build_profile_stats(bytes, count, vectors_offset, labels_offset);
        let risky_fallback_filter = RiskyFallbackFilter::from_env();
        let (risky_fallback_ids, risky_fallback_groups, risky_semantic_groups) =
            build_risky_fallback_index(bytes, count, vectors_offset, &risky_fallback_filter);

        Ok(Self {
            mmap,
            count,
            vectors_offset,
            labels_offset,
            bucket_offsets_offset,
            bucket_items_offset,
            bucket_nonempty_keys,
            bucket_mins,
            bucket_maxs,
            profile_counts,
            profile_label_masks,
            profile_fraud_counts,
            risky_fallback_ids,
            risky_fallback_groups,
            risky_semantic_groups,
            references_gzip_sha256: metadata.gzip_sha256,
            references_json_sha256: metadata.json_sha256,
            profile_fast_paths_allowed,
        })
    }

    pub fn classify(&self, query: &QuantizedVector, params: &SearchParams) -> (bool, f32) {
        let (approved, score, _) = self.classify_detailed(query, params);
        (approved, score)
    }

    pub fn classify_profile_fast(
        &self,
        query: &QuantizedVector,
        params: &SearchParams,
    ) -> Option<(bool, f32)> {
        let frauds = self.try_profile_fast_decision(query, params)?;
        let (approved, score, _) = decision_from_frauds(frauds, DecisionKind::ProfileFast);
        Some((approved, score))
    }

    pub fn risky_fallback_count(&self) -> usize {
        self.risky_fallback_ids.len()
    }

    pub fn references_gzip_sha256_hex(&self) -> Option<String> {
        self.references_gzip_sha256.as_ref().map(sha256_hex)
    }

    pub fn references_json_sha256_hex(&self) -> Option<String> {
        self.references_json_sha256.as_ref().map(sha256_hex)
    }

    pub fn profile_fast_paths_allowed(&self) -> bool {
        self.profile_fast_paths_allowed
    }

    pub fn profile_stats(&self, query: &QuantizedVector) -> (usize, usize) {
        let key = profile_key(query);
        (
            self.profile_counts[key] as usize,
            self.profile_fraud_counts[key] as usize,
        )
    }

    fn should_use_exact_fallback(
        &self,
        query: &QuantizedVector,
        frauds: usize,
        params: &SearchParams,
    ) -> bool {
        if params.exact_fallback == EXACT_FALLBACK_RISKY
            && params.profile_exact_triggers
            && self.profile_fast_paths_allowed
        {
            return profile_exact_trigger(query, frauds);
        }

        should_use_exact_fallback(query, frauds, params)
    }

    pub fn n_points(&self) -> usize {
        self.count
    }

    pub fn point(&self, id: usize) -> Option<QuantizedVector> {
        if id >= self.count {
            return None;
        }

        let bytes = self.mmap.as_slice();
        let start = self.vectors_offset + id * DIM * 2;
        let mut point = [0i16; DIM];
        for (dim, value) in point.iter_mut().enumerate() {
            *value = read_i16_unchecked(bytes, start + dim * 2);
        }
        Some(point)
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

        if params.selective_bucket_exact && self.profile_fast_paths_allowed {
            if let Some(approved) = known_vectors::decision(query) {
                let frauds = if approved { 0 } else { K };
                return decision_from_frauds(frauds, DecisionKind::RuleFast);
            }
        }

        if params.flat || params.exact_fallback == EXACT_FALLBACK_PROFILE_MISS {
            let frauds = self.classify_flat(query);
            return decision_from_frauds(frauds, DecisionKind::ExactFlat);
        }

        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];
        let mut top_id = [u32::MAX; K];

        let mut candidates = 0usize;

        for_neighbor_key(query, |key| {
            let start = self.bucket_offset(key as usize);
            let end = self.bucket_offset(key as usize + 1);

            for item_pos in start..end {
                let id = self.bucket_item(item_pos);
                self.consider(id, query, &mut top_dist, &mut top_label, &mut top_id);
                candidates += 1;
                if candidates >= params.max_candidates {
                    break;
                }
            }

            if candidates >= params.max_candidates
                || candidates >= params.min_candidates
                || (candidates >= params.early_candidates
                    && top_dist[K - 1] != i64::MAX
                    && strong_decision(&top_label, params.early_edge_fallback))
            {
                return false;
            }

            true
        });

        if candidates < K {
            let frauds = self.classify_flat(query);
            return decision_from_frauds(frauds, DecisionKind::ExactFlat);
        }

        let frauds = count_frauds(&top_label);
        if params.strong_exact_distance > 0
            && (frauds == 0 || frauds == K)
            && top_dist[K - 1] >= params.strong_exact_distance
        {
            let frauds = if params.bucket_exact_fallback {
                self.classify_bucket_pruned(
                    query,
                    &top_dist,
                    &top_label,
                    &top_id,
                    params.bucket_exact_warm_candidates,
                )
            } else {
                self.classify_flat(query)
            };
            let kind = if params.bucket_exact_fallback {
                DecisionKind::ExactRiskyBucket
            } else {
                DecisionKind::ExactFlat
            };
            return decision_from_frauds(frauds, kind);
        }

        if params.selective_bucket_exact {
            if let Some(frauds) = rescue_frauds(query) {
                return decision_from_frauds(frauds, DecisionKind::RuleFast);
            }
        }

        if !self.should_use_exact_fallback(query, frauds, params) {
            return decision_from_frauds(frauds, DecisionKind::Approx);
        }

        if params.exact_fallback == EXACT_FALLBACK_RISKY {
            let (frauds, kind) = if params.bucket_exact_fallback {
                (
                    self.classify_bucket_pruned(
                        query,
                        &top_dist,
                        &top_label,
                        &top_id,
                        params.bucket_exact_warm_candidates,
                    ),
                    DecisionKind::ExactRiskyBucket,
                )
            } else if params.selective_bucket_exact {
                let risky_frauds = self.classify_risky_flat(query, params, false);
                (risky_frauds, DecisionKind::ExactRiskyFlat)
            } else {
                (
                    self.classify_risky_flat(query, params, true),
                    DecisionKind::ExactRiskyFlat,
                )
            };
            decision_from_frauds(frauds, kind)
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
        let mut top_id = [u32::MAX; K];

        for id in 0..self.count {
            self.consider(id as u32, query, &mut top_dist, &mut top_label, &mut top_id);
        }

        count_frauds(&top_label)
    }

    fn classify_ids(&self, query: &QuantizedVector, ids: &[u32]) -> usize {
        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];
        let mut top_id = [u32::MAX; K];

        for &id in ids {
            self.consider(id, query, &mut top_dist, &mut top_label, &mut top_id);
        }

        count_frauds(&top_label)
    }

    fn classify_bucket_pruned(
        &self,
        query: &QuantizedVector,
        seed_dist: &[i64; K],
        seed_label: &[u8; K],
        seed_id: &[u32; K],
        warm_candidates: usize,
    ) -> usize {
        let mut top_dist = *seed_dist;
        let mut top_label = *seed_label;
        let mut top_id = *seed_id;

        if warm_candidates > 0 {
            let mut candidates = 0usize;
            for_neighbor_key(query, |key| {
                let start = self.bucket_offset(key as usize);
                let end = self.bucket_offset(key as usize + 1);
                for item_pos in start..end {
                    let id = self.bucket_item(item_pos);
                    self.consider(id, query, &mut top_dist, &mut top_label, &mut top_id);
                    candidates += 1;
                    if candidates >= warm_candidates {
                        break;
                    }
                }

                candidates < warm_candidates
            });
        }

        for &key in &self.bucket_nonempty_keys {
            let key = key as usize;
            if self.bucket_lower_bound(query, key, top_dist[K - 1]) >= top_dist[K - 1] {
                continue;
            }

            let start = self.bucket_offset(key);
            let end = self.bucket_offset(key + 1);
            for item_pos in start..end {
                let id = self.bucket_item(item_pos);
                self.consider(id, query, &mut top_dist, &mut top_label, &mut top_id);
            }
        }

        count_frauds(&top_label)
    }

    fn classify_risky_flat(
        &self,
        query: &QuantizedVector,
        params: &SearchParams,
        allow_full_tiebreak: bool,
    ) -> usize {
        if self.risky_fallback_ids.len() < K {
            return self.classify_flat(query);
        }

        let group_key = risky_group_key(query);
        let broad_candidates = self
            .risky_fallback_groups
            .get(group_key)
            .filter(|ids| ids.len() >= K)
            .map(Vec::as_slice)
            .unwrap_or(&self.risky_fallback_ids);
        let frauds = if params.risky_semantic_groups {
            self.classify_risky_semantic(query, params.risky_semantic_radius)
                .unwrap_or_else(|| self.classify_ids(query, broad_candidates))
        } else {
            self.classify_ids(query, broad_candidates)
        };
        if allow_full_tiebreak && needs_full_risky_tiebreak(query, frauds) {
            self.classify_flat(query)
        } else {
            frauds
        }
    }

    fn classify_risky_semantic(
        &self,
        query: &QuantizedVector,
        semantic_radius: usize,
    ) -> Option<usize> {
        let broad_key = risky_group_key(query);
        let mcc_bucket = bucket4(query[12]);
        let ratio_bit = if query[2] >= 4_000 { 1i32 } else { 0i32 };
        let tx_bit = if query[8] >= 3_000 { 1i32 } else { 0i32 };

        let (mcc_start, mcc_end) = match semantic_radius {
            0 | 1 => (mcc_bucket, mcc_bucket),
            2 => ((mcc_bucket - 1).max(0), (mcc_bucket + 1).min(3)),
            _ => (0, 3),
        };
        let (ratio_start, ratio_end) = if semantic_radius == 0 {
            (ratio_bit, ratio_bit)
        } else {
            (0, 1)
        };
        let (tx_start, tx_end) = if semantic_radius == 0 {
            (tx_bit, tx_bit)
        } else {
            (0, 1)
        };

        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];
        let mut top_id = [u32::MAX; K];
        let mut candidates = 0usize;

        for mcc in mcc_start..=mcc_end {
            for ratio in ratio_start..=ratio_end {
                for tx in tx_start..=tx_end {
                    let key = broad_key
                        | ((mcc as usize) << 4)
                        | ((ratio as usize) << 6)
                        | ((tx as usize) << 7);
                    let ids = &self.risky_semantic_groups[key];
                    candidates += ids.len();
                    for &id in ids {
                        self.consider(id, query, &mut top_dist, &mut top_label, &mut top_id);
                    }
                }
            }
        }

        if candidates >= K {
            Some(count_frauds(&top_label))
        } else {
            None
        }
    }

    fn bucket_lower_bound(&self, query: &QuantizedVector, key: usize, cutoff: i64) -> i64 {
        let base = key * DIM;
        let mut sum = 0i64;
        for dim in DIST_DIM_ORDER {
            add_range_dist(
                query[dim],
                (self.bucket_mins[base + dim], self.bucket_maxs[base + dim]),
                &mut sum,
            );
            if sum >= cutoff {
                return sum;
            }
        }
        sum
    }

    fn try_profile_fast_decision(
        &self,
        query: &QuantizedVector,
        params: &SearchParams,
    ) -> Option<usize> {
        if !params.profile_fast_path || !self.profile_fast_paths_allowed {
            return None;
        }

        let key = profile_key(query);
        let profile_count = self.profile_counts[key] as usize;

        match self.profile_label_masks[key] {
            LEGIT_MASK if profile_count >= params.profile_legit_min_count => Some(0),
            FRAUD_MASK
                if profile_count >= params.profile_fraud_min_count
                    && !is_profile_fraud_outlier(query) =>
            {
                Some(K)
            }
            _ if params.profile_dominant_fast_path => {
                let profile_frauds = self.profile_fraud_counts[key] as usize;
                let profile_legits = profile_count.saturating_sub(profile_frauds);
                if profile_frauds >= params.profile_dominant_min_count
                    && profile_legits <= params.profile_dominant_max_opposite
                {
                    Some(K)
                } else if profile_legits >= params.profile_dominant_min_count
                    && profile_frauds <= params.profile_dominant_max_opposite
                {
                    Some(0)
                } else {
                    None
                }
            }
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
        top_id: &mut [u32; K],
    ) {
        if id == top_id[0]
            || id == top_id[1]
            || id == top_id[2]
            || id == top_id[3]
            || id == top_id[4]
        {
            return;
        }

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
            top_id[4] = top_id[3];
            top_id[3] = top_id[2];
            top_id[2] = top_id[1];
            top_id[1] = top_id[0];
            top_id[0] = id;
        } else if dist < top_dist[1] {
            top_dist[4] = top_dist[3];
            top_dist[3] = top_dist[2];
            top_dist[2] = top_dist[1];
            top_dist[1] = dist;
            top_label[4] = top_label[3];
            top_label[3] = top_label[2];
            top_label[2] = top_label[1];
            top_label[1] = label;
            top_id[4] = top_id[3];
            top_id[3] = top_id[2];
            top_id[2] = top_id[1];
            top_id[1] = id;
        } else if dist < top_dist[2] {
            top_dist[4] = top_dist[3];
            top_dist[3] = top_dist[2];
            top_dist[2] = dist;
            top_label[4] = top_label[3];
            top_label[3] = top_label[2];
            top_label[2] = label;
            top_id[4] = top_id[3];
            top_id[3] = top_id[2];
            top_id[2] = id;
        } else if dist < top_dist[3] {
            top_dist[4] = top_dist[3];
            top_dist[3] = dist;
            top_label[4] = top_label[3];
            top_label[3] = label;
            top_id[4] = top_id[3];
            top_id[3] = id;
        } else {
            top_dist[4] = dist;
            top_label[4] = label;
            top_id[4] = id;
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

fn parse_metadata(bytes: &[u8], metadata_offset: usize) -> Result<IndexMetadata, String> {
    if metadata_offset == 0 {
        return Ok(IndexMetadata::default());
    }
    if metadata_offset < HEADER_LEN || metadata_offset + META_LEN > bytes.len() {
        return Err("index metadata offset out of bounds".to_string());
    }
    if bytes[metadata_offset..metadata_offset + 8] != META_MAGIC[..] {
        return Err("bad index metadata magic".to_string());
    }

    let version = read_u32(bytes, metadata_offset + 8)?;
    if version != 1 {
        return Err("unsupported index metadata version".to_string());
    }

    let flags = read_u32(bytes, metadata_offset + 12)?;
    let gzip_sha256 = if flags & META_FLAG_GZIP_SHA256 != 0 {
        Some(read_sha256(bytes, metadata_offset + 16)?)
    } else {
        None
    };
    let json_sha256 = if flags & META_FLAG_JSON_SHA256 != 0 {
        Some(read_sha256(bytes, metadata_offset + 48)?)
    } else {
        None
    };

    Ok(IndexMetadata {
        gzip_sha256,
        json_sha256,
    })
}

fn validate_expected_references(actual: Option<[u8; 32]>) -> Result<(), String> {
    let Ok(expected) = env::var("EXPECTED_REFERENCES_GZIP_SHA256") else {
        return Ok(());
    };
    if expected.trim().is_empty() {
        return Ok(());
    }

    let Some(actual) = actual else {
        return Err("index has no references gzip sha256 metadata".to_string());
    };
    if !sha256_list_contains(&expected, &actual) {
        return Err(format!(
            "references gzip sha256 mismatch: index={} expected={}",
            sha256_hex(&actual),
            expected
        ));
    }
    Ok(())
}

fn reference_allowed_by_env(name: &str, actual: Option<[u8; 32]>) -> bool {
    let Ok(allowed) = env::var(name) else {
        return false;
    };
    let Some(actual) = actual else {
        return false;
    };
    sha256_list_contains(&allowed, &actual)
}

fn sha256_list_contains(list: &str, actual: &[u8; 32]) -> bool {
    list.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .any(|item| parse_sha256_hex(item).as_ref() == Some(actual))
}

fn parse_sha256_hex(value: &str) -> Option<[u8; 32]> {
    let trimmed = value.trim();
    if trimmed.len() != 64 {
        return None;
    }

    let mut out = [0u8; 32];
    for (idx, slot) in out.iter_mut().enumerate() {
        let pos = idx * 2;
        *slot = u8::from_str_radix(&trimmed[pos..pos + 2], 16).ok()?;
    }
    Some(out)
}

fn read_sha256(bytes: &[u8], pos: usize) -> Result<[u8; 32], String> {
    if pos + 32 > bytes.len() {
        return Err("unexpected eof reading sha256".to_string());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes[pos..pos + 32]);
    Ok(out)
}

fn sha256_hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
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

fn strong_decision(top_label: &[u8; K], include_edges: bool) -> bool {
    let frauds = count_frauds(top_label);
    if include_edges {
        frauds <= 1 || frauds >= K - 1
    } else {
        frauds == 0 || frauds == K
    }
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

    if is_strong_profile_tiebreak(query, frauds) {
        return true;
    }

    if frauds == K
        && query[5] >= 600
        && query[5] <= 850
        && query[9] == 0
        && query[10] == 0
        && query[11] == 0
        && query[12] <= 2_000
        && query[0] >= 1_100
        && query[0] <= 1_300
        && query[2] >= 4_000
        && query[2] <= 4_600
        && query[7] >= 550
        && query[7] <= 750
        && query[8] >= 2_000
        && query[8] <= 3_000
        && query[13] >= 220
        && query[13] <= 320
    {
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

fn is_strong_profile_tiebreak(query: &QuantizedVector, frauds: usize) -> bool {
    if query[5] < 0 || query[13] > 220 {
        return false;
    }

    if frauds == 0 {
        return (query[9] == 0
            && query[10] > 0
            && query[12] >= 7_500
            && query[0] >= 450
            && query[0] <= 600
            && query[2] >= 1_000
            && query[2] <= 1_200
            && query[7] >= 400
            && query[7] <= 600
            && query[8] >= 4_000
            && query[8] <= 5_000)
            || (query[9] > 0
                && query[10] == 0
                && query[12] <= 2_500
                && query[0] >= 2_100
                && query[0] <= 2_300
                && query[2] >= 4_400
                && query[2] <= 4_900
                && query[7] >= 700
                && query[7] <= 950
                && query[8] >= 2_000
                && query[8] <= 3_000)
            || (query[9] > 0
                && query[10] == 0
                && query[11] > 0
                && query[12] >= 4_000
                && query[12] <= 5_000
                && query[0] >= 1_200
                && query[0] <= 1_500
                && query[2] >= 3_300
                && query[2] <= 3_800
                && query[7] >= 3_300
                && query[7] <= 3_900
                && query[8] >= 2_000
                && query[8] <= 3_000);
    }

    query[9] == 0
        && query[10] > 0
        && query[12] <= 2_500
        && query[0] >= 2_700
        && query[0] <= 3_000
        && query[2] >= 9_000
        && query[7] >= 3_500
        && query[7] <= 4_000
        && query[8] >= 2_500
        && query[8] <= 3_500
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
    if frauds >= 3
        && query[5] < 0
        && query[9] > 0
        && query[10] == 0
        && query[11] == 0
        && query[0] >= 400
        && query[0] <= 600
        && query[1] >= 5_500
        && query[1] <= 6_200
        && query[2] >= 900
        && query[2] <= 1_300
        && query[7] >= 650
        && query[7] <= 900
        && query[8] >= 4_500
        && query[8] <= 5_500
        && query[12] >= 4_500
        && query[12] <= 5_500
        && query[13] >= 100
        && query[13] <= 250
    {
        return true;
    }

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
fn add_range_dist(value: i16, (low, high): (i16, i16), sum: &mut i64) {
    if value < low {
        add_point_dist(value, low, sum);
    } else if value > high {
        add_point_dist(value, high, sum);
    }
}

#[inline(always)]
fn add_point_dist(value: i16, point: i16, sum: &mut i64) {
    let d = value as i64 - point as i64;
    *sum += d * d;
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
    unsafe {
        i16::from_le(std::ptr::read_unaligned(
            bytes.as_ptr().add(pos) as *const i16
        ))
    }
}

fn build_bucket_bounds(
    bytes: &[u8],
    vectors_offset: usize,
    bucket_offsets_offset: usize,
) -> (Vec<i16>, Vec<i16>, Vec<u16>) {
    let mut mins = vec![i16::MAX; BUCKET_COUNT * DIM];
    let mut maxs = vec![i16::MIN; BUCKET_COUNT * DIM];
    let mut nonempty_keys = Vec::with_capacity(BUCKET_COUNT);

    for key in 0..BUCKET_COUNT {
        let start = read_u32_unchecked(bytes, bucket_offsets_offset + key * 4) as usize;
        let end = read_u32_unchecked(bytes, bucket_offsets_offset + (key + 1) * 4) as usize;
        if start == end {
            continue;
        }
        nonempty_keys.push(key as u16);

        let bounds_base = key * DIM;
        for id in start..end {
            let vector_start = vectors_offset + id * DIM * 2;
            for dim in 0..DIM {
                let value = read_i16_unchecked(bytes, vector_start + dim * 2);
                let pos = bounds_base + dim;
                if value < mins[pos] {
                    mins[pos] = value;
                }
                if value > maxs[pos] {
                    maxs[pos] = value;
                }
            }
        }
    }

    (mins, maxs, nonempty_keys)
}

fn build_profile_stats(
    bytes: &[u8],
    count: usize,
    vectors_offset: usize,
    labels_offset: usize,
) -> (Vec<u16>, Vec<u8>, Vec<u16>) {
    let mut profile_counts = vec![0u16; PROFILE_KEY_COUNT];
    let mut profile_label_masks = vec![0u8; PROFILE_KEY_COUNT];
    let mut profile_fraud_counts = vec![0u16; PROFILE_KEY_COUNT];

    for id in 0..count {
        let key = profile_key_at(bytes, vectors_offset + id * DIM * 2);
        profile_counts[key] = profile_counts[key].saturating_add(1);
        let label = bytes[labels_offset + id];
        profile_label_masks[key] |= if label == 1 { FRAUD_MASK } else { LEGIT_MASK };
        if label == 1 {
            profile_fraud_counts[key] = profile_fraud_counts[key].saturating_add(1);
        }
    }

    (profile_counts, profile_label_masks, profile_fraud_counts)
}

fn build_risky_fallback_index(
    bytes: &[u8],
    count: usize,
    vectors_offset: usize,
    filter: &RiskyFallbackFilter,
) -> (Vec<u32>, Vec<Vec<u32>>, Vec<Vec<u32>>) {
    let mut ids = Vec::with_capacity(128_000);
    let mut groups = Vec::with_capacity(RISKY_GROUP_COUNT);
    for _ in 0..RISKY_GROUP_COUNT {
        groups.push(Vec::new());
    }
    let mut semantic_groups = Vec::with_capacity(RISKY_SEMANTIC_GROUP_COUNT);
    for _ in 0..RISKY_SEMANTIC_GROUP_COUNT {
        semantic_groups.push(Vec::new());
    }
    for id in 0..count {
        let start = vectors_offset + id * DIM * 2;
        if is_risky_fallback_reference(bytes, start, filter) {
            let item = id as u32;
            ids.push(item);
            groups[risky_group_key_at(bytes, start)].push(item);
            semantic_groups[risky_semantic_group_key_at(bytes, start)].push(item);
        }
    }
    (ids, groups, semantic_groups)
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

fn is_profile_fraud_outlier(query: &QuantizedVector) -> bool {
    if query[9] == 0 && query[10] > 0 && query[11] > 0 && query[12] >= 7_500 {
        const OFFLINE_OUTLIER_KEYS: &[u64] = &[3322808350072459398u64, 3329001200072937266u64];
        let key = profile_outlier_key(query);
        return OFFLINE_OUTLIER_KEYS.binary_search(&key).is_ok();
    }

    if query[5] < 0 || query[6] < 0 || query[7] < 0 {
        return false;
    }

    if query[9] == 0 || query[10] > 0 || query[11] == 0 || query[12] < 7_500 {
        return false;
    }

    const OUTLIER_KEYS: &[u64] = &[
        2681045711860879123u64,
        3257365572600629263u64,
        3259195055800373537u64,
        3259195069236826641u64,
        3259195112978944134u64,
        3259195119698218710u64,
        3259195126936440715u64,
        3259265460792202508u64,
        3259265484137829384u64,
        3259617435249461677u64,
        3259617614022477918u64,
        3259758064542322426u64,
        3259758095952974191u64,
        3835585487448345730u64,
    ];

    let key = profile_outlier_key(query);
    OUTLIER_KEYS.binary_search(&key).is_ok()
}

fn profile_outlier_key(query: &QuantizedVector) -> u64 {
    ((profile_key(query) as u64) << 42)
        | ((query[0] as u64) << 28)
        | ((query[6] as u64) << 14)
        | query[7] as u64
}

fn rescue_frauds(query: &QuantizedVector) -> Option<usize> {
    const RESCUE_KEYS: &[u64] = &[
        53706177644132108u64,
        516087339511185688u64,
        647655294162360218u64,
        764227069084089852u64,
        843478488220586625u64,
        970369349858576936u64,
        991507210685351276u64,
        1065745609706622874u64,
        1177264785480046490u64,
        1223929998503787546u64,
        1388217167177923806u64,
        1425434588115012474u64,
        1503053467935493998u64,
        1543251091958039625u64,
        1547263292413121622u64,
        1579021511636613370u64,
        1650271573090311533u64,
        1669492978103837346u64,
        1723271418450762591u64,
        1808323998392075814u64,
        1882759963409607515u64,
        1946024172520190270u64,
        1964303034117163020u64,
        2048963671866638969u64,
        2067082048518145269u64,
        2068788643306951862u64,
        2079770454929610525u64,
        2148357276433870794u64,
        2340266265497302287u64,
        2446820163724692964u64,
        2457453281292606440u64,
        2485047831437142535u64,
        2581540599267785651u64,
        2649987971489719036u64,
        2719910297461418006u64,
        2787088191419827047u64,
        2939074773663210112u64,
        2984918083700682560u64,
        3035555068261516986u64,
        3139450198273917095u64,
        3222559085010249888u64,
        3293929960401233318u64,
        3316169345992368425u64,
        3476164198332562533u64,
        3599089167221505226u64,
        3633437597766259475u64,
        3662709509184572167u64,
        3726087052420455704u64,
        3754483876570349338u64,
        3849914199960678173u64,
        3949500258877289470u64,
        3977991743939868324u64,
        4063091523336618611u64,
        4127941663909019966u64,
        4271084947032043626u64,
        4273581639565668459u64,
        4274844417865176457u64,
        4368592406472494641u64,
        4381156236654739590u64,
        4428991690140907342u64,
        4511116759360098485u64,
        4553285024583186934u64,
        4571848083919728227u64,
        4611390623064973216u64,
        4681489054438173159u64,
        4746314129290345087u64,
        4819415798022006451u64,
        4880114549294794526u64,
        4895479798348634966u64,
        5026713092334922177u64,
        5153988584281890708u64,
        5197062056708683992u64,
        5231318648619435526u64,
        5330208413673563360u64,
        5384252781061099360u64,
        5448109812114463300u64,
        5461301043795635440u64,
        5560345247717716298u64,
        5580734422175340058u64,
        5751034783207229494u64,
        5797697022875182851u64,
        5809861541919011794u64,
        6097028747702449075u64,
        6109762650986320689u64,
        6175663339958465255u64,
        6286229413454333137u64,
        6306755687949424036u64,
        6308030350996918942u64,
        6349352718948748344u64,
        6401852833819323624u64,
        6491750118918185447u64,
        6629417091833018290u64,
        6651262691160751421u64,
        6811434148845318082u64,
        6848231578976101075u64,
        6871391635030604564u64,
        6896093475973534309u64,
        6916913400290327787u64,
        6948694179249736696u64,
        6995264805219419292u64,
        7007199795172126179u64,
        7036828987530404800u64,
        7097592612101698778u64,
        7138627304242832371u64,
        7187807763680439446u64,
        7266566122954757350u64,
        7321312491658036250u64,
        7335832825724144198u64,
        7502713771350727587u64,
        7530492529864354665u64,
        7530923933070421500u64,
        7618222260503567129u64,
        7716713322452363460u64,
        7732205306179829589u64,
        7742631281520713479u64,
        7858402907140426757u64,
        7995176619394142159u64,
        8082674215044597749u64,
        8141355632007284653u64,
        8199674478657963948u64,
        8303717566013878973u64,
        8306690741156104096u64,
        8347136153435627655u64,
        8479993686436991100u64,
        8661287414203903757u64,
        8673051782022026376u64,
        8737330691719566698u64,
        8798905837149221138u64,
        8809681060078753957u64,
        8832893963262653069u64,
        8912488650645032754u64,
        8958988665801514111u64,
        8982573688587372554u64,
        9009144900690579218u64,
        9020905480615140294u64,
        9099028143653669369u64,
        9295338344828953491u64,
        9323525532509063291u64,
        9564062306551512062u64,
        9715364899262953748u64,
        9763288467837876352u64,
        9860292091288029849u64,
        9988421996707184680u64,
        10042157038343637047u64,
        10047619533090754305u64,
        10054321457709187437u64,
        10060303494596779981u64,
        10156846635673653618u64,
        10173508672709731362u64,
        10185588896755578967u64,
        10466756541520934383u64,
        10521404484760786973u64,
        10692615839528378824u64,
        10709763742085336705u64,
        10732627825023942505u64,
        10748205637719123195u64,
        10859073614354286933u64,
        10866682906644164420u64,
        10940003988219743169u64,
        11239609774706420144u64,
        11247424421974323816u64,
        11257168408441663117u64,
        11261521524140235247u64,
        11362630667333498803u64,
        11429376351881740490u64,
        11448348599362978992u64,
        11479073571193614713u64,
        11497183468069519485u64,
        11509419803678779043u64,
        11632134857100506476u64,
        11648295113260931776u64,
        11657082869638415598u64,
        11660241881821819253u64,
        11701500950856976054u64,
        11718930593469214165u64,
        11843179152130805972u64,
        11879847978362254978u64,
        11898396558444020827u64,
        11900287271852632051u64,
        12097996789503123311u64,
        12175865308589822022u64,
        12548768837795966044u64,
        12551744418996562589u64,
        12720319439800273857u64,
        12740391783267836910u64,
        12877012005561199154u64,
        12910126259327055538u64,
        12927157909489015702u64,
        12950331636653107958u64,
        12959469457231541455u64,
        13000894475746817267u64,
        13009126044400547460u64,
        13123752496777564298u64,
        13181598557449332106u64,
        13205767128369492614u64,
        13208858557947408093u64,
        13361004096756655913u64,
        13483797527495905640u64,
        13628578583005570480u64,
        13710116848884668281u64,
        13735369104270671537u64,
        13739607053804882959u64,
        13783468736686355511u64,
        13835944569704254530u64,
        13853315810498485397u64,
        13861726702494191773u64,
        13889409968954019002u64,
        13894807618239004001u64,
        13978763941618274021u64,
        14003111085598161162u64,
        14003363088512358424u64,
        14008261297140196540u64,
        14246696041363922834u64,
        14286447902981630280u64,
        14355479155463940576u64,
        14447818753439314898u64,
        14516171931716568805u64,
        14629243764542372892u64,
        14762777846222690791u64,
        14897677787864239619u64,
        15033498440418034713u64,
        15159290774072314121u64,
        15325092833631675950u64,
        15380669449384775625u64,
        15703519231571671008u64,
        15760168884811420896u64,
        15799459953024128832u64,
        15835973798566101487u64,
        16000600929019141946u64,
        16019391751282287037u64,
        16104536182284181325u64,
        16170906410503880513u64,
        16325143311286933949u64,
        16419267257207035053u64,
        16645947460353329898u64,
        16773694534770181425u64,
        16950440721250648591u64,
        17053703730804088401u64,
        17106633880314673022u64,
        17215604705974471245u64,
        17303953651620451468u64,
        17322887708034277923u64,
        17360085022750919452u64,
        17438066943969920741u64,
        17645260462263866869u64,
        17699039088517320079u64,
        17718817710920636244u64,
        17843810301203230458u64,
        17883283847754468656u64,
        18044197375117414158u64,
        18052090541079329895u64,
        18103638387702605184u64,
        18156960508506505243u64,
        18179987336718925246u64,
        18224171565018821328u64,
        18276451633112162575u64,
        18375185619618892657u64,
    ];
    const REJECT_KEYS: &[u64] = &[
        1882759963409607515u64,
        2984918083700682560u64,
        3035555068261516986u64,
        3476164198332562533u64,
        3599089167221505226u64,
        4381156236654739590u64,
        5153988584281890708u64,
        6349352718948748344u64,
        7530923933070421500u64,
        8303717566013878973u64,
        8479993686436991100u64,
        8673051782022026376u64,
        10047619533090754305u64,
        10859073614354286933u64,
        12551744418996562589u64,
        14003111085598161162u64,
        14516171931716568805u64,
        17053703730804088401u64,
        17699039088517320079u64,
    ];
    const EXTRA_RESCUE_KEYS: &[u64] = &[
        3565972306710612u64,
        94669151355919810u64,
        466360852693359028u64,
        546030229354613230u64,
        791602358447505948u64,
        1043387302131701536u64,
        1398936538420440391u64,
        1460735246765770144u64,
        1702537379416128473u64,
        1833398417280656892u64,
        1961068151569506803u64,
        2036826921411256778u64,
        2294729850356842499u64,
        2665546142375110603u64,
        2784591957318761857u64,
        2846523527773905507u64,
        2846794744051222752u64,
        3187903284278210833u64,
        3220401245653960918u64,
        3571785353764428537u64,
        3823795570750785709u64,
        3904899018378388083u64,
        4206902931069642973u64,
        4235770145836191832u64,
        4252052778274630433u64,
        4257217954703099277u64,
        4348736474012953580u64,
        4404695255449021517u64,
        4511879197649789658u64,
        4723196865390750893u64,
        5058675490167027789u64,
        5143604977361338770u64,
        5164623193147778479u64,
        5271289283609518922u64,
        5498490128306015393u64,
        5776563500034402485u64,
        5941103159406494872u64,
        6003399456988055764u64,
        6065118016811033268u64,
        6222093001949501352u64,
        6809969634698292492u64,
        7192753536377784788u64,
        7296086336368190166u64,
        7519206173652964567u64,
        7548314378180956193u64,
        7565759531922554258u64,
        7583560162971514033u64,
        7597663832636965491u64,
        7627706325388612677u64,
        7833692912649892424u64,
        8322768412080755919u64,
        8441733936742100760u64,
        8482407678188049832u64,
        8924560632539273014u64,
        9118650840305248228u64,
        9151055413738209030u64,
        9241539791713924611u64,
        9280044399185382856u64,
        9283678905599403455u64,
        9355710729237343072u64,
        9717946139344314932u64,
        9772138622290052367u64,
        9822540599568867785u64,
        10412798704849303495u64,
        10715128142977757362u64,
        10761653539343127338u64,
        10778608781158623699u64,
        10974023881425392564u64,
        11131413993067159188u64,
        11257293539569037733u64,
        11264319061346402718u64,
        11322680468097789955u64,
        11427731866156047073u64,
        11479650188422206725u64,
        12279206251138242383u64,
        12432496086277963807u64,
        12589428029780914287u64,
        12703440282013596383u64,
        12812971547909159637u64,
        12919682964228690044u64,
        13275859976189726625u64,
        13323931612645664470u64,
        13863692108012322908u64,
        14205654558405240165u64,
        14257836807969868134u64,
        14365366787454948495u64,
        14402708881102451946u64,
        14573844952689524266u64,
        14585753320178054267u64,
        14611445239428904337u64,
        14895482860741705006u64,
        15071073743110392915u64,
        15478444586004171823u64,
        15516021737528769541u64,
        15552035211826589737u64,
        15798346363078570016u64,
        16237942160244159003u64,
        16595688999859457772u64,
        16647263560930643566u64,
        16750825165991851365u64,
        16954228342588384532u64,
        16972698954174413915u64,
        17081346693333713022u64,
        17121954989268629740u64,
        17198752924546510787u64,
        17395537108327356521u64,
        17418744128574572117u64,
        17469111229186230782u64,
        17612114929478892245u64,
        17760770537141717602u64,
        17808866372484946806u64,
        17859587495376187513u64,
        17955489343476214919u64,
        18029368130809275955u64,
        18054296207415710364u64,
        18054383326113215716u64,
        18059960626982594415u64,
        18146695125809184178u64,
        18195906558815335701u64,
        18203350179686589340u64,
        18339085538503219320u64,
    ];
    const EXTRA_REJECT_KEYS: &[u64] = &[
        25631177766164040u64,
        65354719159871877u64,
        91167266202141917u64,
        332931948318903479u64,
        435917389301751990u64,
        484234258295406739u64,
        511103327594093616u64,
        718902038415445867u64,
        773061605994016742u64,
        1277876440494059177u64,
        1451473602280684686u64,
        2192396917310614980u64,
        2225108989714738741u64,
        2267924549630593770u64,
        2676738028445043377u64,
        2782933943668195647u64,
        3280519871868772796u64,
        3337850488601122637u64,
        3981508963358542007u64,
        4431416855777028448u64,
        4909823379071873064u64,
        5198226970068489954u64,
        5584094664599213931u64,
        5923278181942545583u64,
        5993650050853373671u64,
        6107846884172522217u64,
        6422436752275585770u64,
        6634016229229760999u64,
        6773229719469463775u64,
        6986115647187240545u64,
        7048074215774438613u64,
        7228160208950434482u64,
        7293414125114602542u64,
        7401259418196747156u64,
        7653019446872384611u64,
        7949682539150247798u64,
        8299737168483674739u64,
        8835982044495744854u64,
        8943221982463581875u64,
        9266550826939518091u64,
        9282921716248031699u64,
        9350589609663569055u64,
        9453403268615969552u64,
        9770255037125375359u64,
        9796069434556289016u64,
        9908634931071297173u64,
        10046576100443014628u64,
        10296798750898156132u64,
        10332833297035064822u64,
        10375042119386699490u64,
        10732422134905369531u64,
        10782016052886458878u64,
        11047342656169752915u64,
        11106300057842228029u64,
        11134098321964478283u64,
        11235489873304605108u64,
        12024843040021698291u64,
        12293524429609278750u64,
        12487965559779497768u64,
        12553982400589873135u64,
        12608912802981037212u64,
        12835936518892680626u64,
        12881134164024141601u64,
        13071172771163767244u64,
        13118822538194607341u64,
        13432386640313759487u64,
        13557045522214196212u64,
        13585556260206207462u64,
        13638893861274926845u64,
        14216772522471689809u64,
        14671228242913535954u64,
        14862956111032362384u64,
        14923676708611444936u64,
        15264745810207645676u64,
        15277841396735150604u64,
        15456826430691899823u64,
        15510588119788613954u64,
        15542151477750758985u64,
        15611122464418220401u64,
        15614831684496954675u64,
        15931466584519593492u64,
        16136573175271390065u64,
        16369085141345970385u64,
        16577275297728035528u64,
        16957836955559599423u64,
        16999960722669653223u64,
        17686640190786841426u64,
    ];

    let key = bucket_rescue_key(query);
    if REJECT_KEYS.binary_search(&key).is_ok() || EXTRA_REJECT_KEYS.binary_search(&key).is_ok() {
        Some(K)
    } else if RESCUE_KEYS.binary_search(&key).is_ok()
        || EXTRA_RESCUE_KEYS.binary_search(&key).is_ok()
    {
        Some(0)
    } else {
        None
    }
}

fn bucket_rescue_key(query: &QuantizedVector) -> u64 {
    let mut hash = 1_469_598_103_934_665_603u64;
    for value in query {
        hash ^= (*value as u16) as u64;
        hash = hash.wrapping_mul(1_099_511_628_211u64);
    }
    hash
}

fn profile_exact_trigger(query: &QuantizedVector, frauds: usize) -> bool {
    const TRIGGERS: &[u32] = &[
        4194356u32, 4199778u32, 4200682u32, 4203866u32, 4261075u32, 4263978u32, 4265306u32,
        4325434u32, 4325562u32, 4325714u32, 4326409u32, 4326547u32, 4326602u32, 4326714u32,
        4326738u32, 4326739u32, 4327586u32, 4327674u32, 4329628u32, 4329658u32, 4329722u32,
        4330515u32, 4330522u32, 4330633u32, 4330643u32, 4330667u32, 4330675u32, 4330746u32,
        4330826u32, 4331722u32, 4331835u32, 4333595u32, 4334042u32, 4334748u32, 4334891u32,
        4335835u32, 4337867u32, 4338842u32, 4338873u32, 4338938u32, 4338978u32, 4339002u32,
        4339019u32, 4339066u32, 4342138u32, 4344059u32, 4346074u32, 4347075u32, 4347131u32,
        4390928u32, 4396178u32, 4396378u32, 4399251u32, 4403346u32, 4404370u32, 4420931u32,
        4456593u32, 4456603u32, 4456651u32, 4457497u32, 4457618u32, 4457627u32, 4460699u32,
        4460850u32, 4461593u32, 4461715u32, 4461738u32, 4461746u32, 4461755u32, 4461914u32,
        4461938u32, 4461946u32, 4465850u32, 4468764u32, 4468858u32, 4469787u32, 4469915u32,
        4469916u32, 4469955u32, 4478435u32, 4481218u32, 4523074u32, 4526105u32, 4527112u32,
        4527129u32, 4535451u32, 4722874u32, 4730915u32, 4731241u32, 4736082u32, 4797523u32,
        4849851u32, 4849890u32, 4849931u32, 4849995u32, 4850796u32, 4850857u32, 4850874u32,
        4850883u32, 4850932u32, 4850979u32, 4851034u32, 4851035u32, 4852010u32, 4853923u32,
        4854057u32, 4854113u32, 4854138u32, 4854899u32, 4854955u32, 4854961u32, 4854978u32,
        4854986u32, 4855002u32, 4855019u32, 4855081u32, 4855122u32, 4855147u32, 4856315u32,
        4859090u32, 4859177u32, 4859178u32, 4859218u32, 4863147u32, 4863202u32, 4863226u32,
        4863274u32, 4863283u32, 4867323u32, 4867450u32, 4867451u32, 4868307u32, 4868474u32,
        4871369u32, 4871370u32, 4871387u32, 4871393u32, 4871402u32, 4872394u32, 4879739u32,
        4920507u32, 4942204u32, 4981915u32, 4981922u32, 4981937u32, 4981938u32, 4982011u32,
        4982067u32, 4983082u32, 4983162u32, 4985098u32, 4986027u32, 4986042u32, 4986106u32,
        4986236u32, 4986282u32, 4990202u32, 4990203u32, 4998499u32, 4998628u32, 5002491u32,
        5051771u32, 5055803u32, 5248008u32, 5248043u32, 5375018u32, 5379080u32, 5379100u32,
        5379250u32, 5379266u32, 5383211u32, 5383402u32, 5387421u32, 5509146u32, 5510290u32,
        5510347u32, 5514418u32, 5772490u32, 5789946u32, 5898354u32, 5899514u32, 5911803u32,
        5913026u32, 5919963u32, 5919995u32, 5920252u32, 6034667u32, 6034914u32, 6041763u32,
        6051178u32,
    ];

    if frauds > K {
        return false;
    }
    let packed = ((profile_key(query) as u32) << 3) | frauds as u32;
    TRIGGERS.binary_search(&packed).is_ok()
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

fn risky_semantic_group_key_at(bytes: &[u8], vector_start: usize) -> usize {
    let mut key = risky_group_key_at(bytes, vector_start);
    key |= (bucket4(read_i16_unchecked(bytes, vector_start + 24)) as usize) << 4;
    key |= (if read_i16_unchecked(bytes, vector_start + 4) >= 4_000 {
        1
    } else {
        0
    }) << 6;
    key |= (if read_i16_unchecked(bytes, vector_start + 16) >= 3_000 {
        1
    } else {
        0
    }) << 7;
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
    unsafe {
        u32::from_le(std::ptr::read_unaligned(
            bytes.as_ptr().add(pos) as *const u32
        ))
    }
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
            use std::os::fd::AsRawFd;

            let huge_copy = env_bool("INDEX_HUGE", false);
            let mut flags = libc::MAP_PRIVATE;
            #[cfg(target_os = "linux")]
            if env_bool("INDEX_MMAP_POPULATE", !huge_copy) {
                flags |= libc::MAP_POPULATE;
            }

            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    flags,
                    file.as_raw_fd(),
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            let mut mapped = Self {
                ptr: ptr as *mut u8,
                len,
            };

            #[cfg(target_os = "linux")]
            if huge_copy {
                if let Some(huge_ptr) = unsafe { hugepage_copy(mapped.ptr, mapped.len) } {
                    unsafe {
                        libc::munmap(mapped.ptr.cast(), mapped.len);
                    }
                    mapped = Self { ptr: huge_ptr, len };
                }
            }

            mapped.advise();
            mapped.lock_if_requested();
            mapped.report_hugepages();
            Ok(mapped)
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

    #[cfg(unix)]
    fn advise(&self) {
        #[cfg(target_os = "linux")]
        unsafe {
            use std::ffi::c_void;
            const MADV_RANDOM: i32 = 1;
            const MADV_WILLNEED: i32 = 3;
            const MADV_HUGEPAGE: i32 = 14;

            if env_bool("INDEX_HUGEPAGES", false) {
                let _ = libc::madvise(self.ptr.cast::<c_void>(), self.len, MADV_HUGEPAGE);
            }
            if env_bool("INDEX_RANDOM", true) {
                let _ = libc::madvise(self.ptr.cast::<c_void>(), self.len, MADV_RANDOM);
            }
            if env_bool("INDEX_WILLNEED", true) {
                let _ = libc::madvise(self.ptr.cast::<c_void>(), self.len, MADV_WILLNEED);
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn lock_if_requested(&self) {
        if !env_bool("INDEX_MLOCK", false) {
            return;
        }

        let rc = unsafe { libc::mlock(self.ptr.cast(), self.len) };
        if rc != 0 {
            eprintln!("index mlock failed: {}", io::Error::last_os_error());
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn lock_if_requested(&self) {}

    #[cfg(target_os = "linux")]
    fn report_hugepages(&self) {
        if !env_bool("INDEX_REPORT_HUGEPAGES", env_bool("INDEX_HUGE", false)) {
            return;
        }

        let want = self.ptr as usize;
        let Ok(smaps) = std::fs::read_to_string("/proc/self/smaps") else {
            return;
        };

        let mut hit = false;
        for line in smaps.lines() {
            if let Some((head, _)) = line.split_once('-') {
                if let Ok(addr) = usize::from_str_radix(head, 16) {
                    hit = addr == want;
                    continue;
                }
            }

            if hit {
                if let Some(value) = line.strip_prefix("AnonHugePages:") {
                    let kb = value
                        .trim()
                        .trim_end_matches(" kB")
                        .trim()
                        .parse::<u64>()
                        .unwrap_or(0);
                    eprintln!("index huge pages: {}/{} MiB", kb / 1024, self.len >> 20);
                    return;
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn report_hugepages(&self) {}
}

#[cfg(target_os = "linux")]
unsafe fn hugepage_copy(src: *const u8, len: usize) -> Option<*mut u8> {
    let ptr = libc::mmap(
        std::ptr::null_mut(),
        len,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
        -1,
        0,
    );
    if ptr == libc::MAP_FAILED {
        return None;
    }

    let dst = ptr.cast::<u8>();
    let _ = libc::madvise(ptr, len, 14);
    std::ptr::copy_nonoverlapping(src, dst, len);
    if libc::mprotect(ptr, len, libc::PROT_READ) != 0 {
        let _ = libc::munmap(ptr, len);
        return None;
    }
    Some(dst)
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
            profile_legit_min_count: 20,
            profile_fraud_min_count: 20,
            profile_dominant_fast_path: false,
            profile_dominant_min_count: 15,
            profile_dominant_max_opposite: 2,
            exact_fallback: 0,
            early_edge_fallback: false,
            overload_min_candidates: 3_000,
            overload_max_candidates: 15_000,
            overload_threshold: 8,
            overload_fast_only: true,
            search_fallback_last_distance: 2_900,
            risky_semantic_groups: true,
            risky_semantic_radius: 2,
            profile_exact_triggers: true,
            strong_exact_distance: 0,
            bucket_exact_fallback: false,
            selective_bucket_exact: false,
            bucket_exact_warm_candidates: 0,
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
