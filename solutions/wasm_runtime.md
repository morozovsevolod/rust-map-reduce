# WASM Runtime

## Wasmtime with fuel consumption

Each WASM execution runs in a fresh Wasmtime instance with fuel enabled. The fuel budget is 10 million base units plus 500 per input byte. This bounds execution time proportionally to input size. A malicious or buggy module that loops forever runs out of fuel and traps. The trap propagates as a task failure, which the scheduler handles with its retry logic.

## File-based I/O

The WASM interface uses raw pointers: `map(input_ptr, input_len, output_ptr) -> output_len`. The host reads the input file, writes it into WASM linear memory, calls the function, then reads the output. This avoids embedding I/O in the WASM module and keeps the interface language-agnostic. The `no_std` task crate uses a bump allocator since it runs on `target_os = "unknown"`.

## Memory layout

WASM linear memory is laid out as static data, then a headroom region for the guest's heap allocations, then the host input buffer, then the host output buffer. The headroom is `max(input_len, 4 MB) * 4` to give the guest enough space for intermediate vectors. The output buffer is 4x the input size, which covers the map case where output includes key delimiters and the reduce case where output is smaller.

Memory grows dynamically when the total needed exceeds the initial 17 pages (1 MB). The `grow` call happens before the function call so the module never sees an out-of-bounds write.

## Bump allocator in WASM

The task crate implements a single-pointer bump allocator using `AtomicUsize`. The host calls `init_heap(heap_start)` once before `map` or `reduce`, passing the address right after `__heap_base`. Allocations move the pointer forward and never free. This is sufficient because each call is independent - memory is discarded after the task completes. A bump allocator is smaller and faster than any general-purpose allocator for single-shot workloads.

## `no_std` and `target_os = "unknown"`

The task crate compiles for `wasm32-unknown-unknown`, which has no OS. Conditionally using `no_std` and `alloc` keeps the binary minimal. The same source file works for both `no_std` (WASM) and `std` (local testing) via `#[cfg]` attributes. This allows unit tests to run natively without a WASM runtime.
