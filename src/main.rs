mod app;
mod api;
mod engine;
mod processor;
mod engine_client;

use log;
use clap::{Arg, Command};
use ::bsread::*;
use std::str::FromStr;
use app::{App};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use clap::builder::Str;
use axum::{extract::State,routing::get,Json, Router,};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::sync::mpsc::channel;
use crate::engine::Engine;

const DEFAULT_PORT:u32 = 15000;

#[derive(Serialize, Clone)]
pub struct Arguments {
    pool_size: usize,
    receivers: usize,
    debug:bool,
    config_path:Option<String>,
    auto_start:bool,
    concurrent:bool,
    disable_handshake:bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    endpoints:Vec<String>,
}

impl Config {
    pub fn update(&mut self, endpoints:Vec<String>)  {
        self.endpoints = endpoints.clone();
    }

    pub fn load(path: &str) -> IOResult<Self> {
        let json =std::fs::read_to_string("config.json")?;
        let config: Config = serde_json::from_str(&json)?;
        Ok(config)
    }

    pub fn save(&self, path: &str) -> IOResult<()> {
        let json = serde_json::to_string_pretty(&self)?;
        std::fs::write("config.json", json)?;
        Ok(())
    }

}

#[macro_export]
macro_rules! exit {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1);
    }};
}

#[tokio::main]
async fn main() {
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::new("Log")
                .short('l')
                .long("log")
                .value_name("LOG")
                .help("Log level (dafault=info)")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help(format!("Port of the API (default={})", DEFAULT_PORT))
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Debug")
                .short('d')
                .long("debug")
                .help("Debug flag")
                .num_args(0), // Does not take a value
        )
        .arg(
            Arg::new("PoolSize")
                .short('s')
                .long("size")
                .help("Maximum endpoints per context (default=100)")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Receivers")
                .short('r')
                .long("receivers")
                .help("Number of receivers per context (default=1)")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("ConfigPath")
                .short('c')
                .long("config")
                .help("Configuration file name")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("AutoStart")
                .short('a')
                .long("auto")
                .help("Auto-start connections")
                .num_args(0), // Does not take a value
        )
        .arg(
            Arg::new("Concurrent")
                .short('t')
                .long("concurrent")
                .help("Does not order sequentially messages from each endpoint")
                .num_args(0), // Does not take a value
        )
        .arg(
            Arg::new("DisableHandshake")
                .short('k')
                .long("disable-handshake")
                .help("Disable handshake check")
                .num_args(0), // Does not take a value
        )

        .get_matches();

    // Check if the help flag is present
    if matches.contains_id("help") {
        exit!("Error: Use -h or --help for instructions.");
    }

    let log_level:String = if let Some(text) = matches.get_one::<String>("Log") {
        text.to_lowercase()
    } else {
        "info".to_string()
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).try_init();


    let port =if let Some(text) = matches.get_one::<String>("Port") {
        match text.parse::<u32>() {
            Ok(number) => number,
            Err(_) => {exit!("Invalid port base value: {}", text);},
        }
    } else {
        DEFAULT_PORT
    };

    let pool_size =if let Some(text) = matches.get_one::<String>("PoolSize") {
        match text.parse::<usize>() {
            Ok(number) => number,
            Err(_) => {exit!("Invalid pool size value: {}", text);},
        }
    } else {
        100
    };

    let receivers =if let Some(text) = matches.get_one::<String>("Receivers") {
        match text.parse::<usize>() {
            Ok(number) => number,
            Err(_) => {exit!("Invalid receivers value: {}", text);},
        }
    } else {
        1
    };

    let config_path= matches.get_one::<String>("ConfigPath").cloned();

    let debug = if matches.get_flag("Debug") {
        true
    }   else {
        false
    };

    let auto_start = if matches.get_flag("AutoStart") {
        true
    }   else {
        false
    };

    let concurrent = if matches.get_flag("Concurrent") {
        true
    }   else {
        false
    };

    let disable_handshake = if matches.get_flag("DisableHandshake") {
        true
    }   else {
        false
    };

    let arguments = Arguments{ pool_size, receivers, debug, config_path, auto_start, concurrent, disable_handshake};
    let app = App::new(arguments.clone());
    let mut app = Arc::new(RwLock::new(app));
    let api = api::init(app.clone());
    let address = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    log::info!("REST API listening on {}", listener.local_addr().unwrap());
    if auto_start {
        app.write().await.start().await.unwrap();
    }
    axum::serve(listener, api).await.unwrap();
}
