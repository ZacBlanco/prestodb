//! Module comprised of functions and structures for implementing the task API
//!

use std::{
    collections::BTreeSet,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicI64},
        Arc,
    },
};

use bytes::{BufMut, Bytes, BytesMut};

use ordered_float::OrderedFloat;

use chrono::Utc;
use log::debug;

use tokio::{
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time::Instant,
};
use uuid::Uuid;

use crate::{
    exec_resources::NodeInfo,
    execution::oxidize::DriverX,
    protocol::{
        page::SerializedPage,
        resources::{
            DateTime, OutputBufferId, OutputBufferInfo, OutputBuffers, PlanFragment, TaskId,
            TaskInfo, TaskState, TaskStatus, TaskUpdateRequest, URI,
        },
    },
};
use anyhow::Result;

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
    output_buffers: RwLock<Option<OutputBuffers>>,
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

#[derive(Debug)]
struct OutputBuffer {
    mailbox: RwLock<UnboundedReceiver<SerializedPage>>,
    sender: UnboundedSender<SerializedPage>,
    current_token: RwLock<u64>,
    pages: RwLock<Vec<Arc<SerializedPage>>>,
    task_state_machine: Arc<TaskStateMachine>,
}

impl OutputBuffer {
    pub fn new(task_state_machine: Arc<TaskStateMachine>) -> Self {
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();
        OutputBuffer {
            mailbox: RwLock::new(recv),
            sender: send,
            pages: RwLock::new(vec![]),
            current_token: RwLock::new(0),
            task_state_machine,
        }
    }

    pub fn get_sender(&self) -> UnboundedSender<SerializedPage> {
        self.sender.clone()
    }

    pub async fn get_output(&self, _id: OutputBufferId, token: u64) -> Result<BufferResult> {
        // first, get any pages sitting in the mailbox buffer
        let mut finished = self.task_state_machine.get_state() == TaskState::FINISHED;
        {
            let cur_tok = *self.current_token.read().await;
            if token < cur_tok && self.pages.read().await.is_empty() {
                return Ok(BufferResult::empty(token, true));
            }
        }

        while (*self.current_token.read().await + self.pages.read().await.len() as u64) <= token {
            finished = self.task_state_machine.get_state() == TaskState::FINISHED;
            // only do try_recv to pull all the pages out if the task is done,
            if finished {
                match self.mailbox.write().await.try_recv() {
                    Ok(page) => self.pages.write().await.push(Arc::new(page)),
                    Err(_) => break,
                }
                continue;
            }
            match self.mailbox.write().await.recv().await {
                Some(page) => {
                    self.pages.write().await.push(Arc::new(page));
                }
                None => {
                    finished = true;
                    break;
                }
            }
        }

        let idx = (token - *self.current_token.read().await) as usize;
        match idx < self.pages.read().await.len() {
            true => {
                debug!("ACCESSING PAGE AT {}", idx);
                let page = self.pages.read().await[idx].clone();
                debug!("SENDING PAGE at {}: {:?}", idx, *page);
                Ok(BufferResult {
                    finished,
                    current_token: token,
                    next_token: token + 1,
                    pages: vec![page],
                })
            }
            false => Ok(BufferResult::EMPTY),
        }
    }

    pub async fn ack_output(&self, _id: OutputBufferId, token: u64) {
        let mut cur_tok = self.current_token.write().await;
        let diff = token as i64 - *cur_tok as i64;
        if diff < 0 {
            return;
        }
        *cur_tok = token;
        self.pages.write().await.drain(0..diff as usize);
    }

    pub async fn clear(&self) {
        *self.current_token.write().await = 0;
        *self.pages.write().await = vec![];
    }
}

pub struct BufferResult {
    finished: bool,
    current_token: u64,
    next_token: u64,
    pages: Vec<Arc<SerializedPage>>,
}

impl BufferResult {
    const EMPTY: BufferResult = BufferResult {
        finished: false,
        current_token: 0,
        next_token: 0,
        pages: vec![],
    };

    pub fn empty(token: u64, finished: bool) -> BufferResult {
        BufferResult {
            finished,
            current_token: token,
            next_token: token,
            pages: vec![],
        }
    }

    pub fn current_token(&self) -> u64 {
        self.current_token
    }
    pub fn next_token(&self) -> u64 {
        self.next_token
    }
    pub fn finished(&self) -> bool {
        self.finished
    }
    pub fn serialize(self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(
            self.pages
                .iter()
                .map(Deref::deref)
                .map(SerializedPage::len)
                .sum(),
        );
        for page in self.pages {
            bytes.put_slice(&(*page)[..])
        }
        bytes.freeze()
    }
}

impl SqlTask {
    pub fn new(id: &TaskId, state: &NodeInfo) -> Self {
        let task_state_machine = Arc::new(TaskStateMachine::new(id));
        SqlTask {
            task_id: id.clone(),
            task_instance_id: TaskInstanceId(Uuid::new_v4()),
            location: create_local_task_location(id, state),
            node_id: state.node_id.clone(),
            output_buffer: OutputBuffer::new(task_state_machine.clone()),
            task_state_machine,
            output_buffers: RwLock::new(None),
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
                .fetch_add(1, std::sync::atomic::Ordering::Acquire),
            state: self.task_state_machine.get_state(),
            selfVar: self.location.clone(),
            completedDriverGroups: BTreeSet::new(),
            failures: vec![],
            queuedPartitionedDrivers: 0,
            runningPartitionedDrivers: 0,
            outputBufferUtilization: OrderedFloat(0.0),
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
            noMoreSplits: BTreeSet::new(),
            stats: Default::default(),
            needsPlan: true,
            metadataUpdates: Default::default(),
            nodeId: self.node_id.clone(),
        }
    }

    pub async fn update(&self, request: TaskUpdateRequest) -> Result<&Self> {
        let plan: PlanFragment = (&(request.fragment.unwrap())).try_into()?;
        let driver = DriverX::new(Arc::new(*plan.root))?;
        let state_machine = self.task_state_machine.clone();
        {
            let mut outputbuffers = self.output_buffers.write().await;
            match *outputbuffers {
                Some(_) => {
                    return Err(anyhow::anyhow!(
                        "output buffers alreadye exist for task {}",
                        self.task_id
                    ))
                }
                None => *outputbuffers = Some(request.outputIds),
            }
        }
        let sender = self.output_buffer.get_sender();
        // launch and detach the driver
        tokio::spawn(async move {
            match driver.start(sender).await {
                Ok(_) => state_machine.finished(),
                Err(e) => state_machine.fail(e.to_string().as_str()),
            }
        });
        Ok(self)
    }

    pub async fn get_result(&self, buffer_id: OutputBufferId, token: u64) -> Result<BufferResult> {
        self.output_buffer.get_output(buffer_id, token).await
    }

    pub async fn ack_result(&self, buffer_id: OutputBufferId, token: u64) -> Result<()> {
        self.output_buffer.ack_output(buffer_id, token).await;
        Ok(())
    }

    pub async fn delete_buffer(&self, _buffer_id: OutputBufferId) {
        self.output_buffer.clear().await;
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
