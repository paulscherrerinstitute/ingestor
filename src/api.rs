use crate::app::*;
use axum::{extract::State,routing::get,Json, Router, };

use std::sync::Arc;
use serde::Serialize;
use tokio::sync::{Mutex};

#[derive(Serialize)]
pub struct Result {
    status: String,
}
async fn state(State(app):  State<Arc<Mutex<App>>>) -> Json<crate::app::State> {
    let app = app.lock().await;
    Json(app.state())
}

async fn status(State(app): State<Arc<Mutex<App>>>) -> Json<Status> {
    let app = app.lock().await;
    Json(app.status())
}

async fn close(State(app): State<Arc<Mutex<App>>>) -> Json<Result> {
    let mut app = app.lock().await;
    app.close();
    Json(Result {
        status: "Ok".to_string(),
    })
}

pub fn init(app:Arc<Mutex<App>>) -> Router {
    let api = Router::new()
        .route("/api/status", get(status))
        .route("/api/state", get(state))
        .route("/api/close", get(close))
        .with_state(app);
    api
}

