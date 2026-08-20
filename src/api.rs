use std::collections::HashMap;
use crate::{Arguments, Config};
use crate::app::{App, Status};
use axum::{extract::State, routing::get, routing::post, routing::put, Json, Router, http::StatusCode, response::{IntoResponse}, Error};
use std::sync::Arc;
use bsread::EndpointDiag;
use serde::Serialize;
use serde_json::json;
use tokio::sync::{ RwLock};


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
pub struct ResponseError {
    error: String,
}
impl ResponseError {
    pub fn get(error: String) -> Json<ResponseError> {
        Json(Self{error})
    }
}


#[derive(Debug)]
pub enum AppError {
    Internal(String),
}
impl AppError {
    pub fn get(error: &str) -> AppError {
        Self::Internal(error.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::Internal(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": message})),
            ).into_response(),
        }
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::Internal(err.to_string())
    }
}

async fn args(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Arguments>, AppError> {
    let app = app.read().await;
    Ok(Json(app.arguments()))
}

async fn state(State(app):  State<Arc<RwLock<App>>>) -> Result<Json<crate::app::State>, AppError> {
    let app = app.read().await;
    Ok(Json(app.state()))
}

async fn status(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Status>, AppError> {
    let app = app.read().await;
    Ok(Json(app.status().await?))
}

async fn diags(State(app): State<Arc<RwLock<App>>>) -> Result<Json<HashMap<String, HashMap<EndpointDiag, u32>>>, AppError> {
    let app = app.read().await;
    Ok(Json(app.diags().await?))
}

async fn config(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Config>, AppError> {
    let app = app.read().await;
    Ok(Json(app.config()))
}

async fn start(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Response>, AppError>  {
    let mut app = app.write().await;
    let status = app.start().await?;
    Ok(Response::ok())
}

async fn stop(State(app): State<Arc<RwLock<App>>>) -> Result<Json<Response>, AppError>  {
    let mut app = app.write().await;
    let status = app.stop().await?;
    Ok(Response::ok())
}

async fn set_config(State(app): State<Arc<RwLock<App>>>,Json(config): Json<Config>,) -> Result<Json<Response>, AppError> {
    let mut app = app.write().await;
    app.set_config(config).await?;
    Ok(Response::ok())
}

pub fn init(app:Arc<RwLock<App>>) -> Router {
    let api = Router::new()
        .route("/api/args", get(args))
        .route("/api/state", get(state))
        .route("/api/status", get(status))
        .route("/api/diags", get(diags))
        .route("/api/config", get(config))
        .route("/api/start", post(start))
        .route("/api/stop", post(stop))
        .route("/api/config", put(set_config))
        .with_state(app);
    api
}

