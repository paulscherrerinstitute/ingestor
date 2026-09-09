mod app;
mod api;
mod engine;
mod processor;
mod engine_client;
mod channel_processor;
mod db;
mod ingestor;
mod arguments;
mod config;

use crate::arguments::Arguments;
use crate::db::DB;
use app::App;
use log;
use log::LevelFilter;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;


#[macro_export]
macro_rules! exit {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1);
    }};
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();

    let arguments = Arc::new(Arguments::parse());

    log::set_max_level(LevelFilter::from_str(&arguments.log_level).unwrap_or(LevelFilter::Info));

    let app = App::new(arguments.clone());
    let mut app = Arc::new(RwLock::new(app));
    let api = api::init(app.clone());
    let address = format!("0.0.0.0:{}", arguments.port);
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    log::info!("REST API listening on {}", listener.local_addr().unwrap());
    if arguments.auto_start {
        app.write().await.start().await.unwrap();
    }
    axum::serve(listener, api).await.unwrap();
}
