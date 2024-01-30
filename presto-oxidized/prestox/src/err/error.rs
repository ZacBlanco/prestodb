use std::fmt::{Debug, Display, Formatter};

use actix_web::ResponseError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Error {
    BackwardsTime,
    MissingParallelism,
    PlanDecode(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl ResponseError for Error {}
