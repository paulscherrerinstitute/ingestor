mod app;
mod api;
mod engine;

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
    capacity: usize,
    receivers: usize,
    debug:bool
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    endpoints:Vec<String>,
}

impl Config {
    fn update(&mut self, endpoints:Vec<String>)  {
        self.endpoints = endpoints.clone();
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
                .help("Log level")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Port of the first sender")
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
            Arg::new("Capacity")
                .short('c')
                .long("capacity")
                .help("Maximum endpoints per context")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Receivers")
                .short('r')
                .long("receivers")
                .help("Number of receivers per context")
                .num_args(1) // Expects one value
                .required(false),
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

    let capacity =if let Some(text) = matches.get_one::<String>("Capacity") {
        match text.parse::<usize>() {
            Ok(number) => number,
            Err(_) => {exit!("Invalid capacity value: {}", text);},
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

    let debug = if matches.get_flag("Debug") {
        true
    }   else {
        false
    };

    let arguments = Arguments{capacity, receivers, debug};
    let app = App::new(arguments.clone());
    //let mut app = Arc::new(app);
    let mut app = Arc::new(RwLock::new(app));
    let api = api::init(app.clone());
    let address = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    log::info!("REST API listening on {}", listener.local_addr().unwrap());
    app.write().await.start().await.unwrap();
    axum::serve(listener, api).await.unwrap();
}
