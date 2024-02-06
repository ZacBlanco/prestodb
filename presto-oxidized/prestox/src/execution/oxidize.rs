use crate::protocol::impls::Page;
use crate::protocol::resources::Block;
use crate::protocol::resources::PlanNode;

use crate::protocol::resources::ScheduledSplit;
use crate::spi::ConnectorPageSource;
use std::fmt::Debug;

use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use log::warn;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct DriverX {
    operators: Vec<Arc<PrestoQueryOperator>>,
    source_operator: Option<Arc<PrestoQueryOperator>>,
    #[allow(unused)]
    graph: Arc<PlanNode>,
    mailbox: Mutex<Receiver<DriverMessage>>,
}

#[derive(Debug)]
pub enum DriverMessage {
    Input(Page),
    Finished,
    RevokeMemory,
    Split(ScheduledSplit),
}

struct QueryGraphDriver {
    op: Arc<dyn QueryGraphOperator>,
    next_op: Sender<DriverMessage>,
    mailbox: Receiver<DriverMessage>,
}

impl Debug for QueryGraphDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryGraphDriver")
            .field("op", &self.op)
            .field("next_op", &self.next_op)
            .field("mailbox", &self.mailbox)
            .finish()
    }
}

impl QueryGraphDriver {}

#[async_trait]
pub trait QueryGraphOperator: Debug {
    async fn handle(&self, message: DriverMessage);
    async fn revoke_memory(&self);
    fn as_source_operator(&self) -> Option<&dyn SourceOperator> {
        None
    }
    fn as_delete_operator(&self) -> Option<&dyn DeleteOperator> {
        None
    }
    fn as_update_operator(&self) -> Option<&dyn UpdateOperator> {
        None
    }
}

#[async_trait]
pub trait SourceOperator: QueryGraphOperator {
    fn add_split(&self, split: ScheduledSplit);
    fn no_more_splits(&self);
    async fn start(&self) -> Result<()>;
}
pub trait DeleteOperator: QueryGraphOperator {
    fn set_page_source(&self, source: Arc<Box<dyn Fn() -> Option<Box<dyn ConnectorPageSource>>>>);
}

pub trait UpdateOperator: QueryGraphOperator {
    fn set_page_source(&self, source: Arc<Box<dyn Fn() -> Option<Box<dyn ConnectorPageSource>>>>);
}

fn explore_query_graph(
    graph: Arc<PlanNode>,
    result_address: Sender<DriverMessage>,
) -> Vec<PrestoQueryOperator> {
    let mut ops = vec![];
    let (send, recv) = channel(10);
    match graph.get_sources() {
        Some(sources) => {
            // add yourself
            ops.push(PrestoQueryOperator {
                node: graph.clone(),
                post_office: result_address,
                mailbox: Mutex::new(recv),
            });
            // now add your children
            for op in sources {
                ops.append(&mut explore_query_graph(Arc::new(op.clone()), send.clone()));
            }
        }
        None => {
            ops.push(PrestoQueryOperator {
                node: graph.clone(),
                post_office: result_address,
                mailbox: Mutex::new(recv),
            });
        }
    }
    ops
}

#[derive(Debug)]
struct PrestoQueryOperator {
    node: Arc<PlanNode>,
    post_office: Sender<DriverMessage>,
    mailbox: Mutex<Receiver<DriverMessage>>,
}

impl PrestoQueryOperator {
    async fn run_operator(&self) -> Result<()> {
        loop {
            match self.mailbox.lock().await.recv().await {
                Some(msg) => match msg {
                    DriverMessage::Input(page) => {
                        self.handle(DriverMessage::Input(page)).await;
                    }
                    DriverMessage::Finished => {
                        if let Err(e) = self.post_office.send(DriverMessage::Finished).await {
                            warn!("{:?} failed to send finished message - next op already closed?: {:?}", self, e);
                            break;
                        }
                    }
                    DriverMessage::RevokeMemory => self.revoke_memory().await,
                    DriverMessage::Split(split) => {
                        if let Some(source_op) = self.as_source_operator() {
                            source_op.add_split(split)
                        } else {
                            warn!("Split added to non-source operator: {:?}", self);
                        }
                    }
                },
                None => {
                    debug!("{:?} receiving channel closed", self);
                    break;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl QueryGraphOperator for PrestoQueryOperator {
    fn as_source_operator(&self) -> Option<&dyn SourceOperator> {
        match self.node.as_ref() {
            PlanNode::ValuesNode { .. } => Some(self),
            _ => None,
        }
    }

    async fn handle(&self, message: DriverMessage) {
        match self.node.as_ref() {
            PlanNode::ValuesNode {
                location: _,
                id: _,
                outputVariables: _,
                rows: _,
                valuesNodeLabel: _,
            } => match message {
                DriverMessage::Input(_) => todo!(),
                DriverMessage::Finished => todo!(),
                DriverMessage::RevokeMemory => todo!(),
                DriverMessage::Split(_) => todo!(),
            },
            _node => (),
        }
    }

    async fn revoke_memory(&self) {
        todo!()
    }
}

#[async_trait]
impl SourceOperator for PrestoQueryOperator {
    fn add_split(&self, _split: ScheduledSplit) {
        todo!()
    }

    fn no_more_splits(&self) {
        todo!()
    }

    async fn start(&self) -> Result<()> {
        let new_post = self.post_office.clone();
        let new_node = self.node.clone();
        let result = match new_node.as_ref() {
            PlanNode::ValuesNode {
                location: _,
                id: _,
                outputVariables: _,
                rows,
                valuesNodeLabel: _,
            } => {
                let blocks = rows
                    .iter()
                    .map(|row| row.iter().map(|_expr| Block).collect::<Vec<_>>())
                    .map(|_| Block("".to_string()))
                    .collect::<Vec<_>>();
                let position_count = blocks.len() as u32;
                tokio::spawn(async move {
                    let page = Page {
                        blocks,
                        position_count,
                        size_in_bytes: 0,
                        retained_size_in_bytes: 0,
                        logical_size_in_bytes: 0,
                    };
                    let _ = new_post.send(DriverMessage::Input(page)).await;
                });
                Ok(())
            }
            a => Err(anyhow!("SourceOperators not implemented for {:?}", a)),
        };
        let _ = self.post_office.send(DriverMessage::Finished).await;
        result
    }
}

impl DriverX {
    pub fn new(query_graph: Arc<PlanNode>) -> Result<Self> {
        let (sender, receiver) = channel(10);
        let all_operators = explore_query_graph(query_graph.clone(), sender)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();
        let mut source_operator: Option<Arc<PrestoQueryOperator>> = None;
        let _delete_operator: Option<Arc<PrestoQueryOperator>> = None;
        let _update_operator: Option<Arc<PrestoQueryOperator>> = None;
        for op in all_operators.iter() {
            if let Some(_operator) = op.as_source_operator() {
                match source_operator {
                    Some(_) => return Err(anyhow!("there must be at most one source operator")),
                    None => source_operator = Some(op.clone()),
                }
            }

            if let Some(_operator) = op.as_delete_operator() {
                match source_operator {
                    Some(_) => return Err(anyhow!("there must be at most one delete operator")),
                    None => source_operator = Some(op.clone()),
                }
            }
            if let Some(_operator) = op.as_update_operator() {
                match source_operator {
                    Some(_) => return Err(anyhow!("there must be at most one update operator")),
                    None => source_operator = Some(op.clone()),
                }
            }
        }
        Ok(DriverX {
            graph: query_graph,
            operators: all_operators,
            source_operator,
            mailbox: Mutex::new(receiver),
        })
    }

    pub async fn start(&self) {
        debug!("executing plan: {:#?}", self.graph);
        if self.source_operator.is_none() {
            warn!("no source operator found for execution");
            return;
        }
        let source_op = self.source_operator.as_ref().unwrap().clone();
        let start_handle = tokio::spawn(async move { source_op.start().await });
        let mut drivers = vec![];
        debug!("{:#?}", self.operators);
        for driver in self.operators.iter() {
            if driver.as_source_operator().is_none() {
                let driver = driver.clone();
                drivers.push(tokio::spawn(async move { driver.run_operator().await }));
            }
        }

        debug!(
            "plan execution: source: {:#?}, drivers: {:#?}",
            self.source_operator, drivers
        );

        loop {
            match self.mailbox.lock().await.recv().await {
                Some(DriverMessage::Finished) => {
                    debug!("Final execution is done");
                    break;
                }
                Some(page_result) => {
                    debug!("driver got message: {:?}", page_result);
                }
                None => {
                    debug!("mailbox finished");
                    break;
                }
            }
        }

        let source_result = start_handle.await;
        debug!(
            "plan source {:?} finished with {:?}",
            self.source_operator, source_result
        );
        let _r = futures::future::join_all(drivers).await;
        debug!("Finished awaiting driver executions");
    }
}
