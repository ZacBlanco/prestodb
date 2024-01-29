//! Module comprised of functions and structures for implementing the task API
//!

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicI64},
        Arc,
    },
};

use chrono::Utc;
use log::debug;

use tokio::{sync::RwLock, time::Instant};
use uuid::Uuid;

use crate::{
    protocol::resources::{
        DateTime, OutputBufferInfo, TaskId, TaskInfo, TaskState, TaskStatus, TaskUpdateRequest, URI,
    },
    resources::NodeInfo,
};

#[derive(Debug)]
struct TaskInstanceId(Uuid);

#[derive(Debug, Clone)]
struct TaskStateMachine {
    id: TaskId,
    state: Arc<std::sync::RwLock<TaskState>>,
    failures: Arc<std::sync::RwLock<Vec<String>>>,
}

#[allow(unused)]
impl TaskStateMachine {
    pub fn new(id: &TaskId) -> Self {
        TaskStateMachine {
            id: id.clone(),
            state: Arc::new(std::sync::RwLock::new(TaskState::RUNNING)),
            failures: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
    }

    pub fn get_state(&self) -> TaskState {
        self.state.read().unwrap().clone()
    }

    pub fn finished(&self) {
        self.transition_to_done_state(TaskState::FINISHED);
    }

    pub fn cancel(&self) {
        self.transition_to_done_state(TaskState::CANCELED)
    }

    pub fn abort(&self) {
        self.transition_to_done_state(TaskState::ABORTED)
    }

    pub fn fail(&self, cause: &str) {
        self.transition_to_done_state(TaskState::FAILED);
        self.failures.write().unwrap().push(cause.to_string());
    }

    fn transition_to_done_state(&self, new_state: TaskState) {
        let mut state = self.state.write().unwrap();
        match *state {
            TaskState::PLANNED | TaskState::RUNNING => {
                *state = new_state;
                debug!("{:?} changed state to {:?}", self.id, state);
            }
            _ => (),
        }
    }
}

#[derive(Debug)]
struct OutputBuffer;
#[derive(Debug)]
struct SqlTaskExecutionFactory;
#[derive(Debug)]
struct QueryContext;
#[derive(Debug)]
struct TaskExchangeClientManager;
#[derive(Debug)]

struct TaskHolder;

#[allow(unused)]
#[derive(Debug)]
pub struct SqlTask {
    task_id: TaskId,
    task_instance_id: TaskInstanceId,
    location: URI,
    node_id: String,
    task_state_machine: Arc<TaskStateMachine>,
    output_buffer: OutputBuffer,
    query_context: QueryContext,
    sql_task_execution_factory: SqlTaskExecutionFactory,
    task_exchange_client_manager: TaskExchangeClientManager,
    last_heartbeat: std::sync::RwLock<DateTime>,
    next_task_info_version: AtomicI64,
    task_holder: RwLock<TaskHolder>,
    needs_plan: AtomicBool,
    creation_time: Instant,
}

impl SqlTask {
    pub fn new(id: &TaskId, state: &NodeInfo) -> Self {
        SqlTask {
            task_id: id.clone(),
            task_instance_id: TaskInstanceId(Uuid::new_v4()),
            location: create_local_task_location(id, state),
            node_id: state.node_id.clone(),
            task_state_machine: Arc::new(TaskStateMachine::new(id)),
            output_buffer: OutputBuffer,
            query_context: QueryContext,
            sql_task_execution_factory: SqlTaskExecutionFactory,
            task_exchange_client_manager: TaskExchangeClientManager,
            last_heartbeat: std::sync::RwLock::new(DateTime(Utc::now())),
            next_task_info_version: AtomicI64::new(1),
            task_holder: RwLock::new(TaskHolder),
            needs_plan: AtomicBool::new(false),
            creation_time: Instant::now(),
        }
    }

    pub fn get_task_status(&self) -> TaskStatus {
        let id_bits = self.task_instance_id.0.as_u64_pair();
        TaskStatus {
            taskInstanceIdLeastSignificantBits: id_bits.0 as i64,
            taskInstanceIdMostSignificantBits: id_bits.1 as i64,
            version: self
                .next_task_info_version
                .fetch_add(1, std::sync::atomic::Ordering::Acquire) as i64,
            state: self.task_state_machine.get_state(),
            selfVar: self.location.clone(),
            completedDriverGroups: HashSet::new(),
            failures: vec![],
            queuedPartitionedDrivers: 0,
            runningPartitionedDrivers: 0,
            outputBufferUtilization: 0.0,
            outputBufferOverutilized: false,
            physicalWrittenDataSizeInBytes: 0,
            memoryReservationInBytes: 1024 * 1024 * 128,
            systemMemoryReservationInBytes: 1024 * 1024,
            peakNodeTotalMemoryReservationInBytes: 0,
            fullGcCount: 0,
            fullGcTimeInMillis: 0,
            totalCpuTimeInNanos: 9001,
            taskAgeInMillis: (Instant::now() - self.creation_time).as_millis() as i64,
            queuedPartitionedSplitsWeight: 100,
            runningPartitionedSplitsWeight: 100,
        }
    }

    pub fn get_task_info(&self, _summarize: bool) -> TaskInfo {
        {
            *self.last_heartbeat.write().unwrap() = DateTime(Utc::now());
        }
        TaskInfo {
            taskId: self.task_id.clone(),
            taskStatus: self.get_task_status(),
            lastHeartbeat: { self.last_heartbeat.read().unwrap().clone() },
            outputBuffers: OutputBufferInfo::new(),
            noMoreSplits: HashSet::new(),
            stats: Default::default(),
            needsPlan: true,
            metadataUpdates: Default::default(),
            nodeId: self.node_id.clone(),
        }
    }

    pub fn update(&self, _request: TaskUpdateRequest) -> &Self {
        self
    }

    pub fn abort(&self) {
        self.task_state_machine.abort()
    }

    pub fn cancel(&self) {
        self.task_state_machine.cancel()
    }
}

fn create_local_task_location(id: &TaskId, state: &NodeInfo) -> URI {
    URI(format!(
        "http://{}/v1/task/{}",
        state.external_address,
        String::from(id)
    ))
}
