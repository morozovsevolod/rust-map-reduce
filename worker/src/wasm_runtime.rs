use std::fs;
use wasmtime::{Config, Engine, Instance, Linker, Memory, Module, Store, Val};

const FUEL_PER_BYTE: u64 = 500;
const FUEL_BASE: u64 = 10_000_000;

/// Execute the `map` function from a WASM module.
/// Reads input file, calls map(ptr, len, out_ptr), writes result to output_path.
pub fn execute_map(wasm_bytes: &[u8], input_path: &str, output_path: &str) -> Result<u64, String> {
    let input = fs::read(input_path)
        .map_err(|e| format!("failed to read input file {}: {}", input_path, e))?;

    let engine = make_engine();
    let module = Module::new(&engine, wasm_bytes)
        .map_err(|e| format!("failed to load wasm module: {}", e))?;

    let mut store: Store<()> = Store::new(&engine, ());
    set_fuel(&mut store, &input).map_err(|e| format!("failed to set fuel: {}", e))?;
    let linker = Linker::<()>::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("failed to instantiate wasm: {}", e))?;

    init_wasm_heap(&mut store, &instance)
        .map_err(|e| format!("failed to init wasm heap: {}", e))?;

    let (input_off, output_off) = setup_memory(&mut store, &instance, &input)
        .map_err(|e| format!("memory setup failed: {}", e))?;

    let memory =
        get_memory(&instance, &mut store).map_err(|e| format!("no memory export: {}", e))?;
    memory
        .write(&mut store, input_off, &input)
        .map_err(|e| format!("failed to write input to memory: {}", e))?;

    let map_func = instance
        .get_func(&mut store, "map")
        .ok_or_else(|| "wasm does not export 'map'".to_string())?;

    let mut results = vec![Val::I32(0)];
    map_func
        .call(
            &mut store,
            &[
                Val::I32(input_off as i32),
                Val::I32(input.len() as i32),
                Val::I32(output_off as i32),
            ],
            &mut results,
        )
        .map_err(|e| format!("map execution failed: {}", e))?;

    let output_len = results[0].i32().unwrap() as usize;

    let output = read_memory(&mut store, &memory, output_off, output_len)
        .map_err(|e| format!("failed to read map output: {}", e))?;

    let output_dir = std::path::Path::new(output_path)
        .parent()
        .ok_or_else(|| "invalid output path".to_string())?;
    fs::create_dir_all(output_dir).map_err(|e| format!("failed to create output dir: {}", e))?;

    fs::write(output_path, &output).map_err(|e| format!("failed to write output file: {}", e))?;

    Ok(output.len() as u64)
}

/// Execute the `reduce` function from a WASM module.
/// Reads sorted input from a single file (pre-shuffled + sorted by task_loop), calls reduce, writes result.
pub fn execute_reduce(
    wasm_bytes: &[u8],
    input_path: &str,
    output_path: &str,
) -> Result<u64, String> {
    let input = fs::read(input_path)
        .map_err(|e| format!("failed to read input file {}: {}", input_path, e))?;

    let engine = make_engine();
    let module = Module::new(&engine, wasm_bytes)
        .map_err(|e| format!("failed to load wasm module: {}", e))?;

    let mut store: Store<()> = Store::new(&engine, ());
    set_fuel(&mut store, &input).map_err(|e| format!("failed to set fuel: {}", e))?;
    let linker = Linker::<()>::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("failed to instantiate wasm: {}", e))?;

    init_wasm_heap(&mut store, &instance)
        .map_err(|e| format!("failed to init wasm heap: {}", e))?;

    let (input_off, output_off) = setup_memory(&mut store, &instance, &input)
        .map_err(|e| format!("memory setup failed: {}", e))?;

    let memory =
        get_memory(&instance, &mut store).map_err(|e| format!("no memory export: {}", e))?;
    memory
        .write(&mut store, input_off, &input)
        .map_err(|e| format!("failed to write input to memory: {}", e))?;

    let reduce_func = instance
        .get_func(&mut store, "reduce")
        .ok_or_else(|| "wasm does not export 'reduce'".to_string())?;

    let mut results = vec![Val::I32(0)];
    reduce_func
        .call(
            &mut store,
            &[
                Val::I32(input_off as i32),
                Val::I32(input.len() as i32),
                Val::I32(output_off as i32),
            ],
            &mut results,
        )
        .map_err(|e| format!("reduce execution failed: {}", e))?;

    let output_len = results[0].i32().unwrap() as usize;

    let output = read_memory(&mut store, &memory, output_off, output_len)
        .map_err(|e| format!("failed to read reduce output: {}", e))?;

    let output_dir = std::path::Path::new(output_path)
        .parent()
        .ok_or_else(|| "invalid output path".to_string())?;
    fs::create_dir_all(output_dir).map_err(|e| format!("failed to create output dir: {}", e))?;

    fs::write(output_path, &output).map_err(|e| format!("failed to write output file: {}", e))?;

    Ok(output.len() as u64)
}

/// Create an engine with fuel consumption enabled so WASM can't loop forever.
fn make_engine() -> Engine {
    let mut config = Config::new();
    config.consume_fuel(true);
    Engine::new(&config).expect("failed to create wasmtime engine")
}

/// Set a fuel budget proportional to input size.
fn set_fuel(store: &mut Store<()>, input: &[u8]) -> Result<(), String> {
    let fuel = FUEL_BASE + input.len() as u64 * FUEL_PER_BYTE;
    store
        .set_fuel(fuel)
        .map_err(|e| format!("fuel not enabled: {}", e))
}

/// Memory layout:
/// [0 .. __heap_base]     - WASM static data
/// [__heap_base .. H+HR]  - Guest heap (HR = headroom)
/// [H+HR .. H+HR+IS]      - Host input buffer
/// [H+HR+IS .. end]       - Host output buffer (4x input size)
///
/// Returns (input_offset, output_offset) in bytes.
fn setup_memory(
    store: &mut Store<()>,
    instance: &Instance,
    input: &[u8],
) -> Result<(usize, usize), String> {
    let heap_base = get_heap_base(&mut *store, instance)
        .map_err(|e| format!("failed to read __heap_base: {}", e))? as usize;

    let heap_headroom = std::cmp::max(input.len(), 4 * 1024 * 1024) * 4;
    let input_off = heap_base + heap_headroom;
    let output_off = input_off + input.len();
    let total_needed = output_off + input.len().saturating_mul(4).max(1024);

    let memory =
        get_memory(instance, &mut *store).map_err(|e| format!("no memory export: {}", e))?;

    let current_size = memory.data_size(&*store);
    if total_needed > current_size {
        let pages_to_grow = (total_needed - current_size).div_ceil(65536) as u64;
        memory
            .grow(&mut *store, pages_to_grow)
            .map_err(|e| format!("failed to grow memory: {}", e))?;
    }

    Ok((input_off, output_off))
}

/// Read `__heap_base` global from the instance.
fn get_heap_base(store: &mut Store<()>, instance: &Instance) -> Result<u64, String> {
    let heap_base_global = instance
        .get_export(&mut *store, "__heap_base")
        .and_then(|e| e.into_global())
        .ok_or_else(|| "__heap_base not exported".to_string())?;

    let val = heap_base_global.get(store);
    val.i32()
        .map(|v| v as u64)
        .ok_or_else(|| "__heap_base is not i32".to_string())
}

/// Call guest `init_heap(__heap_base)` to initialize the bump allocator.
fn init_wasm_heap(store: &mut Store<()>, instance: &Instance) -> Result<(), String> {
    let heap_base = get_heap_base(store, instance)? as i32;

    let init_func = instance
        .get_func(&mut *store, "init_heap")
        .ok_or_else(|| "wasm does not export 'init_heap'".to_string())?;

    let mut results: Vec<Val> = Vec::new();
    init_func
        .call(&mut *store, &[Val::I32(heap_base)], &mut results)
        .map_err(|e| format!("init_heap failed: {}", e))?;

    Ok(())
}

/// Get the exported memory from the instance.
fn get_memory(instance: &Instance, store: &mut Store<()>) -> Result<Memory, String> {
    instance
        .get_memory(store, "memory")
        .ok_or_else(|| "wasm does not export 'memory'".to_string())
}

/// Read `len` bytes from WASM linear memory at `offset`.
fn read_memory(
    store: &mut Store<()>,
    memory: &Memory,
    offset: usize,
    len: usize,
) -> Result<Vec<u8>, String> {
    let data = memory.data(store);
    if offset + len > data.len() {
        return Err(format!(
            "memory read out of bounds: offset={}, len={}, data_len={}",
            offset,
            len,
            data.len()
        ));
    }
    Ok(data[offset..offset + len].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wasm_bytes() -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("target/wasm32-unknown-unknown/release/task.wasm");
        fs::read(&path)
            .unwrap_or_else(|_| panic!("task.wasm not found at {}. Build with: cargo build -p task --target wasm32-unknown-unknown --release", path.display()))
    }

    #[test]
    fn test_execute_map() {
        let wasm = wasm_bytes();
        let tmp = std::env::temp_dir().join(format!("wasm_test_map_{}", std::process::id()));
        let input_path = tmp.with_extension("in");
        let output_path = tmp.with_extension("out");

        fs::write(&input_path, "banana\napple\ncherry\n\n  banana  \n")
            .expect("failed to write input");

        let size = execute_map(
            &wasm,
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .expect("map failed");

        assert!(size > 0, "map should produce output");
        let output = fs::read_to_string(&output_path).expect("failed to read output");

        assert!(output.contains("banana\t1\n"));
        assert!(output.contains("apple\t1\n"));
        assert!(output.contains("cherry\t1\n"));

        fs::remove_file(&input_path).ok();
        fs::remove_file(&output_path).ok();
    }

    #[test]
    fn test_execute_reduce() {
        let wasm = wasm_bytes();
        let tmp = std::env::temp_dir().join(format!("wasm_test_reduce_{}", std::process::id()));
        let input_path = tmp.with_extension("in");
        let output_path = tmp.with_extension("out");

        // Sorted, pre-shuffled input as the framework provides it
        fs::write(&input_path, "apple\t1\nbanana\t1\ncherry\t1\n").expect("failed to write input");

        let size = execute_reduce(
            &wasm,
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .expect("reduce failed");

        assert!(size > 0, "reduce should produce output");
        let output = fs::read_to_string(&output_path).expect("failed to read output");

        assert_eq!(output, "apple\nbanana\ncherry\n");

        fs::remove_file(&input_path).ok();
        fs::remove_file(&output_path).ok();
    }

    #[test]
    fn test_memory_growth() {
        let wasm = wasm_bytes();
        let tmp = std::env::temp_dir().join(format!("wasm_test_growth_{}", std::process::id()));
        let input_path = tmp.with_extension("in");
        let output_path = tmp.with_extension("out");

        // 64K lines, ~300KB — exceeds initial 17-page memory
        let large_input: String = (0..64 * 1024).map(|i| format!("line_{:04}\n", i)).collect();
        fs::write(&input_path, &large_input).expect("failed to write input");

        let size = execute_map(
            &wasm,
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .expect("map failed on large input");

        assert!(size > 0, "map should handle large input via memory growth");

        let output =
            String::from_utf8(fs::read(&output_path).expect("failed to read output")).unwrap();
        assert!(output.contains("line_0000\t1\n"));
        assert!(output.contains("line_0100\t1\n"));

        fs::remove_file(&input_path).ok();
        fs::remove_file(&output_path).ok();
    }
}
