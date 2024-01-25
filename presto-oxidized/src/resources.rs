use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::err::error::Error;
use crate::err::error::Error::BackwardsTime;
use crate::presto_protocol::{
    DataSize, Duration, MemoryAllocation, MemoryInfo, MemoryPoolId, MemoryPoolInfo, NodeState,
    NodeStatus, NodeVersion, QueryId, ServerInfo,
};

#[derive(Clone, Debug)]
pub struct AppState {
    pub node_info: NodeInfo,
    pub node_version: NodeVersion,
    pub node_config: NodeConfig,
    pub node_state: Arc<Mutex<NodeState>>,
    pub memory_manager: Arc<Mutex<LocalMemoryManager>>,
}

impl AppState {
    pub fn new() -> Result<Self, Error> {
        Ok(AppState {
            node_info: NodeInfo::new()?,
            node_version: NodeVersion {
                version: "<unknown>".to_string(),
            },
            node_config: NodeConfig::default(),
            node_state: Arc::new(Mutex::new(NodeState::ACTIVE)),
            memory_manager: Arc::new(Mutex::new(LocalMemoryManager::default())),
        })
    }
}

impl From<&AppState> for ServerInfo {
    fn from(value: &AppState) -> Self {
        ServerInfo {
            nodeVersion: value.node_version.clone(),
            environment: "".to_string(),
            coordinator: false,
            starting: false,
            uptime: None,
        }
    }
}

impl TryFrom<&AppState> for NodeStatus {
    type Error = Error;
    fn try_from(value: &AppState) -> Result<Self, Error> {
        Ok(NodeStatus {
            nodeId: value.node_info.node_id.clone(),
            nodeVersion: value.node_version.clone(),
            environment: value.node_info.environment.clone(),
            coordinator: value.node_config.is_coordinator,
            uptime: Duration(
                SystemTime::now()
                    .duration_since(
                        UNIX_EPOCH
                            + std::time::Duration::from_millis(value.node_info.start_time as u64),
                    )
                    .map_err(|_| BackwardsTime)?
                    .as_millis(),
            ),
            externalAddress: "<unknown>".to_string(),
            internalAddress: "<unknown>".to_string(),
            memoryInfo: MemoryInfo {
                totalNodeMemory: DataSize(0),
                pools: Default::default(),
            },
            processors: num_cpus::get() as i32,
            processCpuLoad: 0.0,
            systemCpuLoad: 0.0,
            heapUsed: 0,
            heapAvailable: 0,
            nonHeapUsed: 0,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    pub environment: String,
    pub pool: String,
    pub node_id: String,
    pub location: String,
    pub binary_spec: String,
    pub config_spec: String,
    pub instance_id: String,
    pub internal_address: String,
    pub external_address: String,
    pub bind_ip: SocketAddr,
    pub start_time: u128,
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
            coordinator_url: "http://127.0.0.1:49463".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub is_coordinator: bool,
    pub coordinator_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    id: String,
    #[serde(rename = "type")]
    service_type: String,
    properties: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Announcement {
    environment: String,
    pool: String,
    location: String,
    services: Vec<Service>,
}

impl Announcement {
    pub fn new(info: &NodeInfo) -> Self {
        let mut props = HashMap::new();
        props.insert("node_version".to_string(), "testversion".to_string());
        props.insert("coordinator".to_string(), false.to_string());
        props.insert(
            "connectorIds".to_string(),
            "system,tpch,iceberg".to_string(),
        );
        props.insert("http".to_string(), "http://localhost:9090".to_string());
        Self {
            environment: "testing".to_string(),
            pool: "general".to_string(),
            location: info.location.clone(),
            services: vec![Service {
                id: info.node_id.clone(),
                service_type: "presto".to_string(),
                properties: props,
            }],
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPoolAssignment {
    pub queryId: QueryId,
    pub poolId: MemoryPoolId,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPoolAssignmentsRequest {
    coordinatorId: String,
    version: i64,
    assignments: Vec<MemoryPoolAssignment>,
}

#[derive(Debug)]
pub struct MemoryPool {
    id: MemoryPoolId,
    max_bytes: i64,
    reserved_bytes: Mutex<i64>,
    reserved_revocable_bytes: Mutex<i64>,
    query_memory_reservations: HashMap<QueryId, i64>,
    tagged_memory_allocations: HashMap<QueryId, HashMap<String, i64>>,
    query_memory_revocable_reservations: HashMap<QueryId, i64>,
}

impl MemoryPool {
    fn new(name: &str, max_bytes: i64) -> Self {
        MemoryPool {
            id: MemoryPoolId(name.to_string()),
            max_bytes,
            reserved_bytes: Default::default(),
            reserved_revocable_bytes: Default::default(),
            query_memory_reservations: Default::default(),
            tagged_memory_allocations: Default::default(),
            query_memory_revocable_reservations: Default::default(),
        }
    }

    async fn get_pool_info(&self) -> MemoryPoolInfo {
        MemoryPoolInfo {
            maxBytes: self.max_bytes,
            reservedBytes: *self.reserved_bytes.lock().await,
            reservedRevocableBytes: *self.reserved_revocable_bytes.lock().await,
            queryMemoryReservations: self.query_memory_reservations.clone(),
            queryMemoryAllocations: self.get_allocations(),
            queryMemoryRevocableReservations: self.query_memory_revocable_reservations.clone(),
        }
    }

    fn get_allocations(&self) -> HashMap<QueryId, Vec<MemoryAllocation>> {
        let mut map = HashMap::new();
        for (query_id, allocations) in self.tagged_memory_allocations.iter() {
            let allocs = allocations
                .iter()
                .map(|(tag, alloc)| MemoryAllocation {
                    tag: tag.clone(),
                    allocation: *alloc,
                })
                .collect::<Vec<_>>();
            map.insert(query_id.clone(), allocs);
        }
        map
    }
}

#[derive(Debug)]
pub struct LocalMemoryManager {
    max_memory: i64,
    pools: HashMap<MemoryPoolId, MemoryPool>,
}

impl Default for LocalMemoryManager {
    fn default() -> Self {
        let mut pools = HashMap::new();
        let general_pool = MemoryPoolId("general".to_string());
        pools.insert(
            general_pool.clone(),
            MemoryPool::new(&general_pool.0, 1024 * 1024 * 1024),
        );
        LocalMemoryManager {
            max_memory: pools.values().map(|x| x.max_bytes).sum(),
            pools,
        }
    }
}

impl LocalMemoryManager {
    pub async fn get_info(&self) -> MemoryInfo {
        let mut info = HashMap::new();
        for (k, v) in self.pools.iter() {
            info.insert(k.clone(), v.get_pool_info().await);
        }
        MemoryInfo {
            totalNodeMemory: DataSize(self.max_memory),
            pools: info,
        }
    }

    pub async fn get_pool_info(&self, pool: &MemoryPoolId) -> Option<MemoryPoolInfo> {
        match self.pools.get(pool) {
            Some(p) => Some(p.get_pool_info().await),
            None => None,
        }
    }
}
