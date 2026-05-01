use crate::vector::{distance_sq, neighbor_keys, QuantizedVector, BUCKET_COUNT, DIM, K};
use std::env;
use std::fs::File;
use std::io;
use std::path::Path;

const MAGIC: &[u8; 8] = b"RINHA26I";
const HEADER_LEN: usize = 80;

#[derive(Clone, Copy)]
pub struct SearchParams {
    pub min_candidates: usize,
    pub max_candidates: usize,
    pub flat: bool,
    pub fast_path: bool,
    pub fast_only: bool,
    pub overload_min_candidates: usize,
    pub overload_max_candidates: usize,
    pub overload_threshold: usize,
    pub overload_fast_only: bool,
}

impl SearchParams {
    pub fn from_env() -> Self {
        let min_candidates = env_usize("MIN_CANDIDATES", 30_000);
        let max_candidates = env_usize("MAX_CANDIDATES", 120_000).max(min_candidates);
        let overload_min_candidates = env_usize("OVERLOAD_MIN_CANDIDATES", 3_000);
        let overload_max_candidates =
            env_usize("OVERLOAD_MAX_CANDIDATES", 15_000).max(overload_min_candidates);

        Self {
            min_candidates,
            max_candidates,
            flat: env::var("SEARCH_MODE")
                .map(|v| v == "flat")
                .unwrap_or(false),
            fast_path: env_bool("FAST_PATH", true),
            fast_only: env_bool("FAST_ONLY", false),
            overload_min_candidates,
            overload_max_candidates,
            overload_threshold: env_usize("OVERLOAD_THRESHOLD", 8),
            overload_fast_only: env_bool("OVERLOAD_FAST_ONLY", true),
        }
    }

    pub fn for_load(&self, load: usize) -> Self {
        if self.overload_threshold == 0 || load < self.overload_threshold || self.flat {
            return *self;
        }

        let mut params = *self;
        params.min_candidates = self.overload_min_candidates.min(self.min_candidates);
        params.max_candidates = self.overload_max_candidates.min(self.max_candidates);
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

        Ok(Self {
            mmap,
            count,
            vectors_offset,
            labels_offset,
            bucket_offsets_offset,
            bucket_items_offset,
        })
    }

    pub fn classify(&self, query: &QuantizedVector, params: &SearchParams) -> (bool, f32) {
        let (approved, score, _) = self.classify_detailed(query, params);
        (approved, score)
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
    ) -> (bool, f32, bool) {
        if params.fast_path || params.fast_only {
            if let Some(result) = fast_classify(query) {
                return (result.0, result.1, true);
            }
        }
        if params.fast_only {
            return (true, 0.0, false);
        }

        let mut top_dist = [i64::MAX; K];
        let mut top_label = [0u8; K];

        if params.flat {
            for id in 0..self.count {
                self.consider(id as u32, query, &mut top_dist, &mut top_label);
            }
        } else {
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

                if candidates >= params.max_candidates || candidates >= params.min_candidates {
                    break;
                }
            }

            if candidates < K {
                for id in 0..self.count {
                    self.consider(id as u32, query, &mut top_dist, &mut top_label);
                }
            }
        }

        let frauds = top_label.iter().filter(|&&label| label == 1).count();
        let score = frauds as f32 / K as f32;
        (score < 0.6, score, false)
    }

    fn consider(
        &self,
        id: u32,
        query: &QuantizedVector,
        top_dist: &mut [i64; K],
        top_label: &mut [u8; K],
    ) {
        let vector = self.vector(id as usize);
        let dist = distance_sq(query, &vector);
        for pos in 0..K {
            if dist < top_dist[pos] {
                for shift in (pos + 1..K).rev() {
                    top_dist[shift] = top_dist[shift - 1];
                    top_label[shift] = top_label[shift - 1];
                }
                top_dist[pos] = dist;
                top_label[pos] = self.label(id as usize);
                break;
            }
        }
    }

    fn vector(&self, id: usize) -> [i16; DIM] {
        let start = self.vectors_offset + id * DIM * 2;
        let bytes = self.mmap.as_slice();
        let mut out = [0i16; DIM];
        for (i, slot) in out.iter_mut().enumerate().take(DIM) {
            let pos = start + i * 2;
            *slot = i16::from_le_bytes([bytes[pos], bytes[pos + 1]]);
        }
        out
    }

    fn label(&self, id: usize) -> u8 {
        self.mmap.as_slice()[self.labels_offset + id]
    }

    fn bucket_offset(&self, key: usize) -> usize {
        let pos = self.bucket_offsets_offset + key * 4;
        read_u32_unchecked(self.mmap.as_slice(), pos) as usize
    }

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

    if likely_fraud_shape(v) {
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

fn read_u32_unchecked(bytes: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
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
    use super::{fast_classify, SearchParams};
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
            1_600, 4_167, 5_000, 6_000, 5_000, 500, 400, 1_000, 3_500, 0, 10_000, 0, 5_000, 2_000,
        ];

        assert_eq!(fast_classify(&vector), Some((false, 1.0)));
    }

    #[test]
    fn overload_switches_to_fast_only() {
        let params = SearchParams {
            min_candidates: 10_000,
            max_candidates: 40_000,
            flat: false,
            fast_path: true,
            fast_only: false,
            overload_min_candidates: 3_000,
            overload_max_candidates: 15_000,
            overload_threshold: 8,
            overload_fast_only: true,
        };

        assert!(!params.for_load(7).fast_only);
        let overloaded = params.for_load(8);
        assert!(overloaded.fast_only);
        assert_eq!(overloaded.min_candidates, 3_000);
        assert_eq!(overloaded.max_candidates, 15_000);
    }
}
