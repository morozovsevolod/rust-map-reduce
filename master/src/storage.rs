use std::io::{BufRead, Read, Seek, Write};
use std::path::{Path, PathBuf};

const CHUNK_MIN: u64 = 16 * 1024 * 1024; // 16 MB
const CHUNK_MAX: u64 = 64 * 1024 * 1024; // 64 MB

/// Create the directory structure for a job.
/// Returns (chunks_dir, wasm_dir).
pub fn create_job_dirs(job_id: &str, data_dir: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
    let job_dir = data_dir.join(job_id);
    let chunks_dir = job_dir.join("chunks");
    let wasm_dir = job_dir.join("wasm");
    let output_dir = job_dir.join("output");

    std::fs::create_dir_all(&chunks_dir)?;
    std::fs::create_dir_all(&wasm_dir)?;
    std::fs::create_dir_all(&output_dir)?;

    Ok((chunks_dir, wasm_dir))
}

/// Write input bytes to a single file on disk.
pub fn write_input(input: &[u8], job_id: &str, data_dir: &Path) -> std::io::Result<PathBuf> {
    let input_file = data_dir.join(job_id).join("input.bin");
    std::fs::write(&input_file, input)?;
    Ok(input_file)
}

/// Split a file into 16-64 MB chunks respecting line boundaries.
/// Writes chunks into `chunks/` subdirectory of the input's parent directory.
/// Returns the chunk file paths.
pub fn split_into_chunks(input_path: &Path, chunks_dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let file = std::fs::File::open(input_path)?;
    let meta = file.metadata()?;
    let total = meta.len();

    if total <= CHUNK_MIN {
        let out = chunks_dir.join("chunk_0");
        std::fs::copy(input_path, &out)?;
        return Ok(vec![out]);
    }

    let mut final_chunks = Vec::new();
    let mut pos: u64 = 0;

    loop {
        let mut end = std::cmp::min(pos + CHUNK_MAX, total);

        // Find line boundary near CHUNK_MAX
        if pos + CHUNK_MAX < total {
            let search_start = (pos + CHUNK_MAX).saturating_sub(8192);
            let search_end = pos + CHUNK_MAX;
            let buf = {
                let mut f = std::fs::File::open(input_path)?;
                let mut buf = vec![0u8; (search_end - search_start) as usize];
                f.seek(std::io::SeekFrom::Start(search_start))?;
                f.read_exact(&mut buf)?;
                buf
            };
            if let Some(nl) = buf.iter().rposition(|&b| b == b'\n') {
                end = search_start + nl as u64 + 1;
            }
        }

        let chunk_idx = final_chunks.len();
        let out = chunks_dir.join(format!("chunk_{chunk_idx}"));
        {
            let mut in_f = std::fs::File::open(input_path)?;
            let mut out_f = std::fs::File::create(&out)?;
            in_f.seek(std::io::SeekFrom::Start(pos))?;
            let len = (end - pos) as usize;
            let mut buf = vec![0u8; std::cmp::min(len, 1 << 20)];
            let mut remaining = len;
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, buf.len());
                let n = in_f.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                out_f.write_all(&buf[..n])?;
                remaining -= n;
            }
        }
        final_chunks.push(out);
        pos = end;

        if pos >= total {
            break;
        }
    }

    Ok(final_chunks)
}

/// Generate random bytes and write to disk in one pass.
pub fn generate_and_write(size_mb: u32, job_id: &str, data_dir: &Path) -> std::io::Result<PathBuf> {
    use rand::Rng;

    let job_dir = data_dir.join(job_id);
    std::fs::create_dir_all(&job_dir)?;
    let input_file = job_dir.join("input.bin");
    let total_bytes = size_mb as usize * 1024 * 1024;

    let mut file = std::fs::File::create(&input_file)?;
    let mut rng = rand::thread_rng();
    let buf_size = 1 << 20; // 1 MB write buffer
    let mut buf = vec![0u8; buf_size];

    let mut remaining = total_bytes;
    while remaining > 0 {
        let to_fill = std::cmp::min(remaining, buf_size);
        let slice = &mut buf[..to_fill];

        let mut pos = 0;
        while pos < to_fill {
            let word_len = rng.gen_range(3..=20);
            for byte in slice[pos..std::cmp::min(pos + word_len as usize, to_fill)].iter_mut() {
                *byte = rng.gen_range(b'a'..=b'z');
            }
            pos += word_len as usize;
            if pos < to_fill {
                slice[pos] = b'\n';
                pos += 1;
            }
        }

        file.write_all(&buf[..to_fill])?;
        remaining -= to_fill;
    }

    Ok(input_file)
}

/// Save WASM module to disk, keyed by hash.
pub fn save_wasm(wasm: &[u8], wasm_hash: &str, data_dir: &Path) -> std::io::Result<PathBuf> {
    let wasm_dir = data_dir.join("wasm");
    std::fs::create_dir_all(&wasm_dir)?;
    let wasm_file = wasm_dir.join(format!("{wasm_hash}.wasm"));
    if !wasm_file.exists() {
        std::fs::write(&wasm_file, wasm)?;
    }
    Ok(wasm_file)
}

/// Compute a fast hash for a WASM module.
pub fn wasm_hash(bytes: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut h = DefaultHasher::new();
    h.write(bytes);
    format!("{:016x}", h.finish())
}

/// Remove a job's data directory. Called by cleanup after result download completes.
pub fn remove_job_data(job_id: &str, data_dir: &Path) -> std::io::Result<()> {
    let job_dir = data_dir.join(job_id);
    if job_dir.exists() {
        std::fs::remove_dir_all(&job_dir)?;
    }
    Ok(())
}

/// K-way merge of sorted line-delimited files into a single output file.
/// Uses minimal memory — reads one line per input at a time.
pub fn merge_sorted_files(inputs: &[PathBuf], output: &Path) -> std::io::Result<()> {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    #[derive(Eq, PartialEq)]
    struct LineEntry {
        line: Vec<u8>,
        source: usize,
    }

    impl Ord for LineEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            // Reverse for min-heap
            other.line.cmp(&self.line)
        }
    }

    impl PartialOrd for LineEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut readers: Vec<std::io::BufReader<std::fs::File>> = inputs
        .iter()
        .filter(|p| p.exists())
        .map(|p| std::io::BufReader::new(std::fs::File::open(p).unwrap()))
        .collect();

    if readers.is_empty() {
        std::fs::write(output, b"")?;
        return Ok(());
    }

    let mut heap = BinaryHeap::new();

    // Prime the heap
    for (i, reader) in readers.iter_mut().enumerate() {
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line)? > 0 {
            heap.push(LineEntry { line, source: i });
        }
    }

    let mut out = std::fs::File::create(output)?;

    while let Some(entry) = heap.pop() {
        out.write_all(&entry.line)?;

        let reader = &mut readers[entry.source];
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line)? > 0 {
            heap.push(LineEntry {
                line,
                source: entry.source,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_small_file() {
        let dir = std::env::temp_dir().join(format!("mr_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("input.bin");
        std::fs::write(&input, b"short input\nline2\n").unwrap();

        let chunks_dir = dir.join("chunks");
        std::fs::create_dir_all(&chunks_dir).unwrap();
        let chunks = split_into_chunks(&input, &chunks_dir).unwrap();
        assert_eq!(chunks.len(), 1);
        let data = std::fs::read(&chunks[0]).unwrap();
        assert_eq!(data, b"short input\nline2\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_generate_and_write() {
        let dir = std::env::temp_dir().join(format!("mr_gen_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let path = generate_and_write(1, "test_job", &dir).unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() >= 1024 * 1024);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_merge_sorted_files() {
        let dir = std::env::temp_dir().join(format!("mr_merge_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let f1 = dir.join("a.txt");
        let f2 = dir.join("b.txt");
        std::fs::write(&f1, b"apple\ncherry\n").unwrap();
        std::fs::write(&f2, b"banana\ngrape\n").unwrap();

        let out = dir.join("merged.txt");
        merge_sorted_files(&[f1, f2], &out).unwrap();

        let result = std::fs::read_to_string(&out).unwrap();
        assert_eq!(result, "apple\nbanana\ncherry\ngrape\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_wasm_hash_deterministic() {
        let bytes = b"\0asm test";
        let h1 = wasm_hash(bytes);
        let h2 = wasm_hash(bytes);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16);
    }
}
