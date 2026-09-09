use std::path::PathBuf;
use bsread::{IOResult, SocketType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub sources: Vec<Source>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SocketKind {
    PUB,
    PUSH,
    PULL,
    SUB,
}

impl From<SocketKind> for SocketType {
    fn from(socket_type: SocketKind) -> Self {
        match socket_type {
            SocketKind::PUB => SocketType::PUB,
            SocketKind::PUSH => SocketType::PUSH,
            SocketKind::PULL => SocketType::PULL,
            SocketKind::SUB => SocketType::SUB,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub address: String,
    #[serde(rename = "type")]
    pub socket_type: Option<SocketKind>,
    pub enabled: Option<bool>,
}

impl Config {
    pub fn update(&mut self, sources:Vec<Source>)  {
        self.sources = sources.clone();
    }
    pub fn load(path: &PathBuf) -> IOResult<Self> {
        let json =std::fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&json)?;
        Ok(config)
    }
    pub fn save(&self, path: &PathBuf) -> IOResult<()> {
        let json = serde_json::to_string_pretty(&self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}