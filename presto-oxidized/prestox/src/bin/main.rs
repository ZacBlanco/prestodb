use std::time::Duration;

use actix_web::middleware::Logger;
use actix_web::{web, App, HttpServer};

use log::{debug, error, info, warn};
use prestox::config::generate_config;
use prestox::discovery;
use prestox::resources::AppState;
use prestox::server::api::worker_config;
use prestox::{self};
use tokio::{signal, time};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();
    let config = generate_config();
    info!(
        "searching for cluster at {:?}",
        prestox::config::DISCOVERY_URI.lookup(&config)
    );
    info!(
        "binding to port {:?}",
        prestox::config::HTTP_SERVER_PORT.lookup(&config)
    );
    let state = web::Data::new(AppState::new(&config).unwrap());
    let web_data = state.clone();
    let heartbeat_data = state.clone();
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
        .bind((
            "127.0.0.1",
            prestox::config::HTTP_SERVER_PORT
                .lookup(&config)
                .expect("port must be set"),
        ))?
        .run(),
    );

    let heartbeat_task = tokio::task::spawn(async move {
        let state = heartbeat_data;
        let mut interval = time::interval(Duration::from_secs(15));
        let discovery_uri = &state.node_config.discovery_uri;
        let client = reqwest::Client::builder().build().unwrap();
        loop {
            interval.tick().await;
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
    let result = futures::join!(heartbeat_task, server_task);
    debug!("shutdown result {:?}", result);
    Ok(())
}
