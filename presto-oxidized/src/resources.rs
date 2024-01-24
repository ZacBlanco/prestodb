use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::err::error::Error;
use crate::err::error::Error::{BackwardsTime, MissingParallelism};

#[derive(Clone)]
pub struct AppState {
    node_info: NodeInfo,
    node_version: NodeVersion,
    node_config: NodeConfig,
}

impl AppState {
    pub fn new() -> Result<Self, Error> {
        Ok(AppState {
            node_info: NodeInfo::new()?,
            node_version: NodeVersion::default(),
            node_config: NodeConfig::default(),
        })
    }
}

impl TryFrom<&AppState> for NodeStatus {
    type Error = Error;
    fn try_from(value: &AppState) -> Result<Self, Error> {
        Ok(NodeStatus {
            node_id: value.node_info.node_id.clone(),
            node_version: value.node_version.clone(),
            environment: value.node_info.environment.clone(),
            coordinator: value.node_config.is_coordinator,
            uptime: SystemTime::now()
                .duration_since(
                    UNIX_EPOCH + Duration::from_millis(value.node_info.start_time as u64),
                )
                .map_err(|_| BackwardsTime)?
                .as_millis(),
            logical_cores: std::thread::available_parallelism()
                .map_err(|_| MissingParallelism)?
                .get() as u16,
            external_address: value.node_info.external_address.clone(),
            internal_address: value.node_info.internal_address.clone(),
            memory_info: Default::default(),
            processors: num_cpus::get_physical() as u16,
            process_cpu_load: 0.0,
            system_cpu_load: 0.0,
            heap_used: 0,
            heap_available: 0,
            non_heap_used: 0,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    environment: String,
    pool: String,
    node_id: String,
    location: String,
    binary_spec: String,
    config_spec: String,
    instance_id: String,
    internal_address: String,
    external_address: String,
    bind_ip: SocketAddr,
    start_time: u128,
}

impl NodeInfo {
    fn new() -> Result<Self, Error> {
        Ok(NodeInfo {
            environment: "<unknown>".to_string(),
            pool: "<unknown>".to_string(),
            node_id: Uuid::new_v4().to_string(),
            location: "<unknown>".to_string(),
            binary_spec: "<unknown>".to_string(),
            config_spec: "<unknown>".to_string(),
            instance_id: "<unknown>".to_string(),
            internal_address: "<unknown>".to_string(),
            external_address: "<unknown>".to_string(),
            bind_ip: SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| BackwardsTime)?
                .as_millis(),
        })
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            is_coordinator: false,
        }
    }
}

#[derive(Clone)]
struct NodeConfig {
    is_coordinator: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeVersion {
    version: String,
}

impl Default for NodeVersion {
    fn default() -> Self {
        NodeVersion {
            version: "<unknown>".to_string(),
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct MemoryInfo {
    total_node_memory: u64,
    pools: HashMap<MemoryPoolId, MemoryPoolInfo>
}



pub type MemoryPoolId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPoolInfo {
    max_bytes: u64,
    reserved_bytes: u64,
    reserved_revocable_bytes: u64,
    query_memory_reservations: HashMap<QueryId, u64>,
    query_memory_allocations: HashMap<QueryId, Vec<MemoryAllocation>>,
    query_memory_revocable_reservations: HashMap<QueryId, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct QueryId {
    id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryAllocation {
    tag: String,
    allocation: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    node_id: String,
    node_version: NodeVersion,
    environment: String,
    coordinator: bool,
    uptime: u128,
    logical_cores: u16,
    external_address: String,
    internal_address: String,
    memory_info: MemoryInfo,
    processors: u16,
    process_cpu_load: f64,
    system_cpu_load: f64,
    heap_used: u64,
    heap_available: u64,
    non_heap_used: u64,
}

pub struct TaskInfo {

}

pub struct TaskStatus {

}
