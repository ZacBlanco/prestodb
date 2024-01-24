use crate::resources::{AppState, NodeStatus};
use actix_web::{get, head, web, Error, HttpResponse, delete, post};
use serde::{Deserialize, Serialize};

pub fn worker_config(config: &mut web::ServiceConfig) {
    config.service(status);
}

type Response = Result<HttpResponse, Error>;

#[get("/status")]
async fn status(state: web::Data<AppState>) -> Response {
    Ok(HttpResponse::Ok().json(TryInto::<NodeStatus>::try_into(state.get_ref())?))
}

#[head("/status")]
async fn status_ping() -> Response {
    Ok(HttpResponse::Ok().finish())
}

/// Control Plane Operations
///

#[post("/task/{task_id}")]
async fn post_task() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[get("/task/{task_id}/status")]
async fn get_task_status() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[get("/task/{task_id}")]
async fn get_task_info() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[delete("/task/{task_id}")]
async fn delete_task() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[get("/task")]
async fn get_tasks() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

/// Data Plane Operations

#[get("/{task_id}/results/{buffer_id}/{token}")]
async fn get_buffer_token() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[get("/{task_id}/results/{buffer_id}/{token}/acknowledge")]
async fn get_buffer_token_ack() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[delete("/{task_id}/results/{buffer_id}/{token}")]
async fn delete_buffer_token() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}
