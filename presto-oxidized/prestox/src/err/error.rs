use std::fmt::{Debug, Display, Formatter};

use actix_web::ResponseError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Error {
    BackwardsTime,
    MissingParallelism,
    PlanDecode(String),
    JsonSerde(String),
    Unimplemented,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl std::error::Error for Error {}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonSerde(format!("{:?}", value))
    }
}

impl ResponseError for Error {}
