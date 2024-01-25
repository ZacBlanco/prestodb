use std::time::Duration;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

use log::{debug, error, info, warn};
use prestox::discovery;
use prestox::resources::AppState;
use prestox::server::api::worker_config;
use prestox::{self};
use tokio::{signal, time};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let state = web::Data::new(AppState::new().unwrap());
    let web_data = state.clone();
    let heartbeat_data = state.clone();
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();
    let server_task = tokio::task::spawn(
        HttpServer::new(move || {
            let data = web_data.clone();
            App::new().service(
                web::scope("/v1")
                    .wrap(Logger::default())
                    .configure(worker_config)
                    .app_data(data),
            )
        })
        .bind(("127.0.0.1", 9090))?
        .run(),
    );

    let heartbeat_task = tokio::task::spawn(async move {
        let state = heartbeat_data;
        let mut interval = time::interval(Duration::from_secs(15));
        let discovery_uri = &state.node_config.coordinator_url;
        let client = reqwest::Client::builder().build().unwrap();
        loop {
            interval.tick().await;
            debug!("sending heartbeat: {:?}", state);
            if let Err(e) = discovery::announce(&client, discovery_uri, &state.node_info).await {
                warn!("Failed to announce to discovery server: {:?}", e);
            } else {
                debug!("sent announcement with {}", state.node_info.node_id);
            }
        }
    });

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("Failed to listen for shutdown signal: {}", err)
        }
    }
    info!("Gracefully shutting down...");
    heartbeat_task.abort();
    server_task.abort();
    Ok(())
}
