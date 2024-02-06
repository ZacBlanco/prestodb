use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use config::Config;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use futures::Future;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use anyhow::anyhow;

use crate::config::{DISCOVERY_URI, HTTP_SERVER_PORT};
use crate::err::error::Error;
use crate::err::error::Error::BackwardsTime;
use crate::protocol::resources::{
    DataSize, Duration, MemoryAllocation, MemoryInfo, MemoryPoolId, MemoryPoolInfo, NodeState,
    NodeStatus, NodeVersion, QueryId, ServerInfo, TaskId, TaskInfo, TaskState, TaskStatus,
    TaskUpdateRequest,
};
use crate::task::SqlTask;

#[derive(Clone, Debug)]
pub struct AppState {
    pub node_info: Arc<NodeInfo>,
    pub node_version: NodeVersion,
    pub node_config: NodeConfig,
    pub node_state: Arc<Mutex<NodeState>>,
    pub memory_manager: Arc<Mutex<LocalMemoryManager>>,
    pub task_manager: Arc<LocalTaskManager>,
}

impl AppState {
    pub fn new(config: &Config) -> Result<Self> {
        let node_info = Arc::new(NodeInfo::new(config)?);
        Ok(AppState {
            node_info: node_info.clone(),
            node_version: NodeVersion {
                version: "<unknown>".to_string(),
            },
            node_config: NodeConfig::new(config),
            node_state: Arc::new(Mutex::new(NodeState::ACTIVE)),
            memory_manager: Arc::new(Mutex::new(LocalMemoryManager::default())),
            task_manager: Arc::new(LocalTaskManager::new(node_info)),
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
            externalAddress: "localhost".to_string(),
            internalAddress: "localhost".to_string(),
            memoryInfo: MemoryInfo {
                totalNodeMemory: DataSize(0),
                pools: Default::default(),
            },
            processors: num_cpus::get() as i32,
            processCpuLoad: OrderedFloat(0.0),
            systemCpuLoad: OrderedFloat(0.0),
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

fn get_location(port: u16) -> String {
    return format!("http://localhost:{}", port);
}

impl NodeInfo {
    fn new(config: &Config) -> Result<Self> {
        let server_port = HTTP_SERVER_PORT
            .lookup(config)
            .ok_or(anyhow!("missing http port config"))?;
        Ok(NodeInfo {
            environment: "testing".to_string(),
            pool: "general".to_string(),
            node_id: Uuid::new_v4().to_string(),
            location: get_location(server_port),
            binary_spec: "<unknown>".to_string(),
            config_spec: "<unknown>".to_string(),
            instance_id: "<unknown>".to_string(),
            internal_address: format!("localhost:{}", server_port),
            external_address: format!("localhost:{}", server_port),
            bind_ip: SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), server_port)),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| BackwardsTime)?
                .as_millis(),
        })
    }
}

impl NodeConfig {
    fn new(config: &Config) -> Self {
        NodeConfig {
            is_coordinator: false,
            discovery_uri: DISCOVERY_URI.lookup(config).expect("need a discovery URI"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub is_coordinator: bool,
    pub discovery_uri: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Service {
    id: String,
    #[serde(rename = "type")]
    service_type: String,
    properties: BTreeMap<String, String>,
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
        let mut props = BTreeMap::new();
        props.insert("node_version".to_string(), "testversion".to_string());
        props.insert("coordinator".to_string(), false.to_string());
        props.insert(
            "connectorIds".to_string(),
            "system,tpch,iceberg".to_string(),
        );
        props.insert("http".to_string(), info.location.to_string());
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
    #[allow(unused)]
    id: MemoryPoolId,
    max_bytes: i64,
    reserved_bytes: Mutex<i64>,
    reserved_revocable_bytes: Mutex<i64>,
    query_memory_reservations: BTreeMap<QueryId, i64>,
    tagged_memory_allocations: BTreeMap<QueryId, BTreeMap<String, i64>>,
    query_memory_revocable_reservations: BTreeMap<QueryId, i64>,
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

    fn get_allocations(&self) -> BTreeMap<QueryId, Vec<MemoryAllocation>> {
        let mut map = BTreeMap::new();
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
    pools: BTreeMap<MemoryPoolId, MemoryPool>,
}

impl Default for LocalMemoryManager {
    fn default() -> Self {
        let mut pools = BTreeMap::new();
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
        let mut info = BTreeMap::new();
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

impl TaskId {
    pub fn to_string(&self) -> String {
        format!(
            "{}.{}.{}.{}.{}",
            self.query_id, self.stage_id, self.stage_execution_id, self.id, self.attempt_number
        )
    }
}

#[derive(Debug)]
pub struct LocalTaskManager {
    tasks: Arc<crossbeam_skiplist::SkipMap<TaskId, SqlTask>>,
    node_info: Arc<NodeInfo>,
}

impl LocalTaskManager {
    pub fn new(node_info: Arc<NodeInfo>) -> Self {
        LocalTaskManager {
            tasks: Arc::new(SkipMap::new()),
            node_info,
        }
    }

    fn get_task(&self, id: &TaskId) -> Entry<'_, TaskId, SqlTask> {
        self.tasks
            .get_or_insert_with(id.clone(), || SqlTask::new(&id, self.node_info.as_ref()))
    }
}

impl<'a> TaskManager for LocalTaskManager {
    fn get_all_task_info(&self, summarize: bool) -> Vec<TaskInfo> {
        self.tasks
            .iter()
            .map(|x| x.value().get_task_info(summarize))
            .collect::<Vec<_>>()
    }

    fn get_task_info(&self, id: &TaskId, summarize: bool) -> TaskInfo {
        self.get_task(&id).value().get_task_info(summarize)
    }

    fn get_task_status(&self, id: &TaskId) -> TaskStatus {
        self.get_task(&id).value().get_task_status()
    }

    fn update_task(
        &self,
        id: &TaskId,
        request: TaskUpdateRequest,
        summarize: bool,
    ) -> Result<TaskInfo> {
        Ok(self
            .get_task(id)
            .value()
            .update(request)?
            .get_task_info(summarize))
    }

    fn get_task_status_future(
        &self,
        _task: &TaskId,
        _current_state: TaskState,
    ) -> Box<dyn Future<Output = TaskStatus>> {
        todo!()
    }

    fn get_task_instance_id(&self, _task: TaskId) -> String {
        todo!()
    }

    fn abort_task(&self, task: &TaskId, summarize: bool) -> TaskInfo {
        let binding = self.get_task(task);
        let task = binding.value();
        task.abort();
        task.get_task_info(summarize)
    }

    fn cancel_task(&self, task: &TaskId, summarize: bool) -> TaskInfo {
        let binding = self.get_task(task);
        let task = binding.value();
        task.cancel();
        task.get_task_info(summarize)
    }
}

pub trait TaskManager {
    fn get_all_task_info(&self, summarize: bool) -> Vec<TaskInfo>;
    fn get_task_info(&self, id: &TaskId, summarize: bool) -> TaskInfo;
    fn get_task_status(&self, id: &TaskId) -> TaskStatus;
    fn get_task_status_future(
        &self,
        task: &TaskId,
        current_state: TaskState,
    ) -> Box<dyn Future<Output = TaskStatus>>;
    fn update_task(
        &self,
        id: &TaskId,
        request: TaskUpdateRequest,
        summarize: bool,
    ) -> Result<TaskInfo>;
    fn get_task_instance_id(&self, task: TaskId) -> String;
    fn abort_task(&self, task: &TaskId, summarize: bool) -> TaskInfo;
    fn cancel_task(&self, task: &TaskId, summarize: bool) -> TaskInfo;
}
