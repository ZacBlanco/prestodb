use crate::presto_protocol::{Duration, MemoryPoolId, NodeStatus, ServerInfo};
use crate::resources::{AppState, MemoryPoolAssignmentsRequest};
use actix_web::{delete, get, head, post, put, web, Error, HttpResponse};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn worker_config(config: &mut web::ServiceConfig) {
    config
    .service(post_memory)
    .service(get_memory_pool)
        .service(status)
        .service(get_info)
        .service(get_state)
        .service(put_state)
        .service(get_coordinator);
}

type Response = Result<HttpResponse, Error>;

/// Memory

#[post("/memory")]
async fn post_memory(
    state: web::Data<AppState>,
    request: web::Json<MemoryPoolAssignmentsRequest>,
) -> Response {
    Ok(HttpResponse::Ok().json(state.memory_manager.lock().await.get_info().await))
}

#[get("/memory/{pool_id}")]
async fn get_memory_pool(state: web::Data<AppState>, path: web::Path<(String,)>) -> Response {
    let args = path.into_inner();
    let info = state
        .memory_manager
        .lock()
        .await
        .get_pool_info(&MemoryPoolId(args.0))
        .await;
    Ok(HttpResponse::Ok().json(info))
}

/// Server Info

#[get("/info")]
async fn get_info(state: web::Data<AppState>) -> Response {
    let info: ServerInfo = ServerInfo {
        coordinator: false,
        environment: state.node_info.environment.clone(),
        nodeVersion: state.node_version.clone(),
        uptime: SystemTime::now()
            .duration_since(
                UNIX_EPOCH + std::time::Duration::from_millis(state.node_info.start_time as u64),
            )
            .ok()
            .map(|x| Duration(x.as_millis())),
        starting: false,
    };
    Ok(HttpResponse::Ok().json(info))
}

#[get("/info/state")]
async fn get_state(state: web::Data<AppState>) -> Response {
    let body;
    {
        body = state.get_ref().node_state.lock().await.clone()
    }
    Ok(HttpResponse::Ok().json(body))
}

#[put("/info/state")]
async fn put_state() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[put("/info/coordinator")]
async fn get_coordinator() -> Response {
    // coordinator not supported on oxidized worker
    Ok(HttpResponse::NotFound().finish())
}

/// Status

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

#[get("/task/async/{task_id}/results/{buffer_id}/{token}")]
async fn get_buffer_token() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[get("/task/async/{task_id}/results/{buffer_id}/{token}/acknowledge")]
async fn get_buffer_token_ack() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}

#[delete("/task/async/{task_id}/results/{buffer_id}/{token}")]
async fn delete_buffer_token() -> Response {
    Ok(HttpResponse::NotImplemented().finish())
}
