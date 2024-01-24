use actix_web::{web, App, HttpServer};

use prestox;
use prestox::resources::AppState;
use prestox::server::api::worker_config;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        let state = AppState::new().unwrap();
        App::new().service(
            web::scope("/v1")
                .configure(worker_config)
                .app_data(web::Data::new(state)),
        )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
