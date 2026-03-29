use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub addr: String,
    pub folder: String,
}

pub fn load_config() -> Result<Config> {
    let s = fs::read_to_string("config.json")?;
    let config = serde_json::from_str(&s)?;
    Ok(config)
}
