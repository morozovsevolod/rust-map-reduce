use anyhow::{Result, anyhow};
use dashmap::DashMap;
use std::sync::Arc;
use wasmtime::component::{Component, Linker, Val};
use wasmtime::{Config, Engine, Store};

pub struct WasmExecutor {
    tasks: DashMap<String, Arc<Component>>,
    folder: String,
    engine: Engine,
    linker: Arc<Linker<()>>,
}

impl WasmExecutor {
    pub fn new(folder: String) -> Result<WasmExecutor> {
        let mut config = Config::new();
        config.wasm_component_model(true);

        let engine = Engine::new(&config)?;
        let linker = Arc::new(Linker::new(&engine));

        let tasks = DashMap::new();

        Ok(WasmExecutor {
            tasks,
            folder,
            engine,
            linker,
        })
    }

    fn get_task(&self, name: &str) -> Result<Arc<Component>> {
        let component = self
            .tasks
            .get(name)
            .ok_or_else(|| anyhow!("No task with name {name} was found."))?
            .clone();

        Ok(component)
    }

    pub fn cache_task(&self, task: &str) -> Result<()> {
        let file = format!("{}/wasm/{task}.wasm", self.folder);
        let component = Component::from_file(&self.engine, file)?;

        self.tasks.insert(task.to_string(), Arc::new(component));

        Ok(())
    }

    pub fn execute_fmap(&self, file: &str, input: Vec<u8>) -> Result<Vec<(String, String)>> {
        let component = self.get_task(file)?;
        let linker = self.linker.clone();

        let mut store = Store::new(&self.engine, ());
        let instance = linker.instantiate(&mut store, &component)?;

        let fmap = instance
            .get_func(&mut store, "fmap")
            .ok_or_else(|| anyhow!("Failed to read `fmap` function from the {file}"))?;

        let mut results = vec![Val::List(Vec::new())];
        fmap.call(
            &mut store,
            &[Val::List(input.iter().map(|b| Val::U8(*b)).collect())],
            &mut results,
        )?;

        let list = match &results[0] {
            Val::List(l) => l.clone(),
            _ => anyhow::bail!("unexpected return type for fmap"),
        };
        let mut out = Vec::new();
        for pair in list {
            if let Val::Tuple(vals) = pair {
                if let (Val::String(k), Val::String(v)) = (&vals[0], &vals[1]) {
                    out.push((k.to_string(), v.to_string()));
                }
            }
        }
        Ok(out)
    }

    pub fn execute_freduce(&self, file: &str, key: String, values: Vec<String>) -> Result<String> {
        let component = self.get_task(file)?;
        let linker = self.linker.clone();

        let mut store = Store::new(&self.engine, ());
        let instance = linker.instantiate(&mut store, &component)?;

        let freduce = instance
            .get_func(&mut store, "freduce")
            .ok_or_else(|| anyhow!("Failed to read `freduce` function from the {file}"))?;

        let mut results = vec![Val::String(String::new().into())];
        let vals = values
            .into_iter()
            .map(|s| Val::String(s.into()))
            .collect::<Vec<_>>();
        freduce.call(
            &mut store,
            &[Val::String(key.into()), Val::List(vals)],
            &mut results,
        )?;

        let msg = match &results[0] {
            Val::String(s) => s.clone(),
            _ => anyhow::bail!("unexpected return type for freduce"),
        };

        Ok(msg)
    }
}
