use std::collections::HashMap;
use std::io::ErrorKind;
use crate::{Arguments};
use crate::app::{App, Status, Stats, Config};
use axum::{extract::State, routing::get, routing::post, routing::put, Json, Router, http::StatusCode, response::{IntoResponse}, Error};
use std::sync::Arc;
use bsread::EndpointDiag;
use serde::Serialize;
use serde_json::json;
use tokio::sync::{ RwLock};

const API_PREFIX: &str = "/api";

#[derive(Serialize)]
pub struct Response {
    status: String,
}
impl Response {
    pub fn new(status: &str)-> Self {
        Self {status:status.to_string()}
    }
    pub fn get(status: &str) -> Json<Response> {
        Json(Self::new(status))
    }
    pub fn ok() -> Json<Response> {
        Self::get("Success")
    }
}


#[derive(Debug)]
pub enum AppError {
    Internal(String, String),
}
impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::Internal(kind, message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"kind": kind, "message": message}})),
            ).into_response(),
        }
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::Internal(err.kind().to_string(), err.to_string())
    }
}

async fn args(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Arguments>, AppError> {
    log::debug!("API call: args");
    let app = app.read().await;
    Ok(Json(app.arguments()))
}

async fn state(State(app):  State<Arc<RwLock<App>>>) -> Result<Json<crate::app::State>, AppError> {
    log::debug!("API call: state");
    let app = app.read().await;
    Ok(Json(app.state()))
}

async fn status(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Status>, AppError> {
    log::debug!("API call: status");
    let app = app.read().await;
    Ok(Json(app.status().await?))
}

async fn diags(State(app): State<Arc<RwLock<App>>>) -> Result<Json<HashMap<String, HashMap<EndpointDiag, u32>>>, AppError> {
    log::debug!("API call: diags");
    let app = app.read().await;
    Ok(Json(app.diags().await?))
}

async fn config(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Config>, AppError> {
    log::debug!("API call: config");
    let app = app.read().await;
    Ok(Json(app.config()))
}

async fn stats(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Stats>, AppError> {
    log::debug!("API call: stats");
    let app = app.read().await;
    Ok(Json(app.stats().await?))
}

async fn log_level(State(app): State<Arc<RwLock<App>>>) -> Result<Json<String>, AppError> {
    log::debug!("API call: log_level");
    let app = app.read().await;
    Ok(Json(app.log_level().await?))
}

async fn start(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Response>, AppError>  {
    log::info!("API call: start");
    let mut app = app.write().await;
    app.start().await?;
    Ok(Response::ok())
}

async fn stop(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Response>, AppError>  {
    log::info!("API call: stop");
    let mut app = app.write().await;
    app.stop().await?;
    Ok(Response::ok())
}

//curl -X PUT --json @cfg.json http://localhost:15000/api/config
//curl -X PUT --json '{"endpoints":["tcp://localhost:12000","tcp://localhost:12001"]}' http://localhost:15000/api/config
async fn set_config(State(app): State<Arc<RwLock<App>>>,Json(config): Json<Config>,) -> Result<Json<Response>, AppError> {
    let s = 2;
    log::info!("API call: set_config {:?}", config);
    let mut app = app.write().await;
    app.set_config(config).await?;
    Ok(Response::ok())
}

async fn reset_stats(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Response>, AppError>  {
    log::info!("API call: reset_stats");
    let mut app = app.write().await;
    let status = app.reset_stats().await?;
    Ok(Response::ok())
}

async fn set_log_level(State(app): State<Arc<RwLock<App>>>,Json(level): Json<String>) -> Result<Json<Response>, AppError>  {
    log::info!("API call: set_log_level:  {}", level);
    let mut app = app.write().await;
    app.set_log_level(level).await?;
    Ok(Response::ok())
}


pub fn init(app:Arc<RwLock<App>>) -> Router {
    let api = Router::new()
        .route("/args", get(args))
        .route("/state", get(state))
        .route("/status", get(status))
        .route("/diags", get(diags))
        .route("/config", get(config))
        .route("/stats", get(stats))
        .route("/log-level", get(log_level))
        .route("/start", post(start))
        .route("/stop", post(stop))
        .route("/reset_stats", post(reset_stats))
        .route("/config", put(set_config))
        .route("/log-level", put(set_log_level));
    let app = Router::new()
        .nest(API_PREFIX, api)
        .with_state(app);
    app
}

