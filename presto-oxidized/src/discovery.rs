use reqwest::{Client, Error, Response};

use crate::{
    presto_protocol::NodeStatus,
    resources::{Announcement, NodeInfo},
};

pub async fn announce(
    client: &reqwest::Client,
    discovery_url: &str,
    node_info: &NodeInfo,
) -> Result<Response, reqwest::Error> {
    let request = client
        .put(format!(
            "{}/v1/announcement/{}",
            discovery_url, &node_info.node_id
        ))
        .json(&Announcement::new(node_info))
        .build()?;
    client.execute(request).await
}

pub async fn do_heartbeat(
    client: &Client,
    discovery_url: &str,
    status: &NodeStatus,
) -> Result<Response, Error> {
    let request = client
        .put(format!("{}/v1/heartbeat", discovery_url))
        .json(status)
        .build()?;
    client.execute(request).await
}
