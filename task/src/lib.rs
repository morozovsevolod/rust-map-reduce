//! Raw core-WASM MapReduce task.
//!
//! Exports `map`, `reduce`, and `init_heap` functions.
//! Host must call `init_heap(__heap_base)` before calling map/reduce.
//!
//! Signature: `(input_ptr, input_len, output_ptr) -> output_len`

#![cfg_attr(target_os = "unknown", no_std)]

#[cfg(target_os = "unknown")]
extern crate alloc;

#[cfg(target_os = "unknown")]
use alloc::vec::Vec;

#[cfg(not(target_os = "unknown"))]
use std::vec::Vec;

#[cfg(target_os = "unknown")]
use core::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_os = "unknown")]
static HEAP_PTR: AtomicUsize = AtomicUsize::new(usize::MAX);

#[cfg(target_os = "unknown")]
struct BumpAlloc;

#[cfg(target_os = "unknown")]
unsafe impl core::alloc::GlobalAlloc for BumpAlloc {
    unsafe fn alloc(&self, layout: core::alloc::Layout) -> *mut u8 {
        loop {
            let ptr = HEAP_PTR.load(Ordering::Relaxed);
            if ptr == usize::MAX {
                return core::ptr::null_mut();
            }
            let aligned = (ptr + 7) & !7;
            let new_ptr = aligned + layout.size();
            match HEAP_PTR.compare_exchange_weak(ptr, new_ptr, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return aligned as *mut u8,
                Err(_) => continue,
            }
        }
    }

    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: core::alloc::Layout) {
        // Bump allocator never frees
    }
}

#[cfg(target_os = "unknown")]
#[global_allocator]
static ALLOC: BumpAlloc = BumpAlloc;

#[cfg(target_os = "unknown")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

/// Initialize the bump allocator. Call once before map/reduce.
#[cfg(target_os = "unknown")]
#[no_mangle]
unsafe extern "C" fn init_heap(heap_start: usize) {
    HEAP_PTR.store(heap_start, Ordering::Relaxed);
}

/// Map phase: emit `"<trimmed_line>\t1\n"` for each non-empty input line.
#[no_mangle]
unsafe extern "C" fn map(input: *const u8, input_len: usize, output: *mut u8) -> usize {
    let data = core::slice::from_raw_parts(input, input_len);
    let input = core::str::from_utf8_unchecked(data);

    let mut result: Vec<u8> = Vec::new();
    for line in input.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            result.extend_from_slice(trimmed.as_bytes());
            result.push(b'\t');
            result.push(b'1');
            result.push(b'\n');
        }
    }

    let out_len = result.len();
    core::ptr::copy_nonoverlapping(result.as_ptr(), output, out_len);
    out_len
}

/// Reduce phase: input is pre-sorted by key with format `key\tvalue\n`.
/// All identical keys are adjacent. This function aggregates grouped keys
/// and emits one output line per key. For word count, that means dedup.
#[no_mangle]
unsafe extern "C" fn reduce(input: *const u8, input_len: usize, output: *mut u8) -> usize {
    let data = core::slice::from_raw_parts(input, input_len);
    let input_str = core::str::from_utf8_unchecked(data);

    let mut result: Vec<u8> = Vec::new();
    let mut key: &str = "";
    let mut have_key = false;

    for line in input_str.lines() {
        let k = line.split('\t').next().unwrap_or(line);
        if have_key && key != k {
            result.extend_from_slice(key.as_bytes());
            result.push(b'\n');
            key = k;
        } else if !have_key {
            key = k;
            have_key = true;
        }
    }
    if have_key {
        result.extend_from_slice(key.as_bytes());
        result.push(b'\n');
    }

    let out_len = result.len();
    core::ptr::copy_nonoverlapping(result.as_ptr(), output, out_len);
    out_len
}
