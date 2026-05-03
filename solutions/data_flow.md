# Data Flow and Storage

## Line-aligned chunk splitting

Input files are split into 16-64 MB chunks. For files smaller than 16 MB, a single chunk is created. The split algorithm scans backward from the 64 MB mark to find the nearest newline within an 8 KB window. This ensures chunks never cut a record in half. The backward scan is bounded so it doesn't degenerate on files with very long lines.

Chunk sizes target the middle ground. Smaller chunks increase the number of map tasks and shuffle overhead. Larger chunks reduce parallelism. The 16-64 MB range matches the original MapReduce paper.

## Random data generation

When no input file is provided, the master generates random lowercase words (3-20 characters) separated by newlines. Generation writes in 1 MB buffers directly to disk, avoiding holding the full file in memory. This path is for testing - the sort task works identically on generated or uploaded data.

## Hash-based partitioning

After a map task produces `key\tvalue\n` lines, the worker partitions output by `hash(key) % R`. Each partition is written to `map_r{N}.out` on the worker's local data directory. The partition files are served via the worker's HTTP data server. This is the classic Hadoop-style partition function - identical keys always land in the same reducer.

The hash uses Rust's `DefaultHasher` (SipHash). It is not cryptographically random, but it distributes keys uniformly within a single process. Since partitioning only needs consistency within one job run (all map tasks use the same function), cross-process randomness is not required.

## Shuffle via HTTP file serving

Reduce tasks receive a list of `DataLocation` entries, each containing a worker's data URL and the relative file path. The reduce worker fetches each file via HTTP, concatenates the data, and sorts by key. The sort happens in memory on the worker. This is efficient because the total data for one partition of one job is typically smaller than the full input - it only contains the subset of keys that hashed to that partition.

## K-way merge on the master

After all reduce tasks complete, the master fetches each reduce output file from the worker's HTTP server. The files are merged using a `BinaryHeap`-based k-way merge. One line is read from each input at a time. The heap keeps the smallest line at the top. Memory usage is constant - one buffer per input file, regardless of total output size.

The merge assumes reduce outputs are already sorted. The sort task's reduce function deduplicates adjacent keys, which produces sorted output. The framework guarantees this because it sorts data before calling the WASM reduce function.

## WASM deduplication by hash

WASM modules are saved on the master under `wasm/{hash}.wasm` using a 16-character hex hash of the module bytes. Before writing, the master checks if the file already exists. Workers cache locally the same way. Resubmitting the same module skips the write on both sides. The hash uses `DefaultHasher` for speed - collisions are negligible for course-sized workloads.

## Disk directory layout

```
{data_dir}/
  {job_id}/
    chunks/chunk_0, chunk_1, ...
    output/result.txt
  wasm/{hash}.wasm
```

Workers mirror a similar structure in their `/tmp/worker_data_{id}/` directory. Raw input files are deleted after chunking - only chunks are kept. The directory layout is flat per-job so cleanup is a single `remove_dir_all`.
