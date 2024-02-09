use crate::protocol::page::BlockBuilder;
use crate::protocol::page::Page;
use crate::protocol::page::PageBuilder;
use crate::protocol::page::SerializedPage;
use crate::protocol::resources::PlanNode;

use crate::protocol::resources::RowExpression::ConstantExpression;
use crate::protocol::resources::ScheduledSplit;
use crate::spi::ConnectorPageSource;
use std::fmt::Debug;

use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Error;
use anyhow::Result;
use async_trait::async_trait;
use log::debug;
use log::error;
use log::warn;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

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

#[allow(clippy::type_complexity)]
pub trait DeleteOperator: QueryGraphOperator {
    fn set_page_source(&self, source: Arc<Box<dyn Fn() -> Option<Box<dyn ConnectorPageSource>>>>);
}

#[allow(clippy::type_complexity)]
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
                post_office: RwLock::new(Some(result_address)),
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
                post_office: RwLock::new(Some(result_address)),
                mailbox: Mutex::new(recv),
            });
        }
    }
    ops
}

#[derive(Debug)]
struct PrestoQueryOperator {
    node: Arc<PlanNode>,
    post_office: tokio::sync::RwLock<Option<Sender<DriverMessage>>>,
    mailbox: Mutex<Receiver<DriverMessage>>,
}

impl PrestoQueryOperator {
    async fn run_operator(&self) -> Result<()> {
        loop {
            let msg = self.mailbox.lock().await.recv().await;
            match msg {
                Some(msg) => match msg {
                    DriverMessage::Input(page) => {
                        self.handle(DriverMessage::Input(page)).await;
                    }
                    DriverMessage::Finished => {
                        // loop over the mailbox to get any more pages, then send the finished message
                        match self.post_office.read().await.deref() {
                            Some(chan) => {
                                if let Err(e) = chan.send(DriverMessage::Finished).await {
                                    warn!("{:?} failed to send finished message - next op already closed?: {:?}", self, e);
                                }
                            }
                            None => {
                                break;
                            }
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
        *self.post_office.write().await = None;
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
        #[allow(clippy::match_single_binding)]
        match self.node.as_ref() {
            _ => {
                // default, just forward message to next operator
                match self.post_office.read().await.deref() {
                    Some(chan) => {
                        if let Err(e) = chan.send(message).await {
                            error!(
                                "operator {:?} errored sending to next operator: {:?}",
                                self.node, e
                            )
                        }
                    }
                    None => warn!(
                        "can't send message {:?} to next op because channel was closed",
                        message
                    ),
                }
            }
        };
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
        let new_post = self.post_office.read().await.clone();
        let new_node = self.node.clone();
        let result = match new_node.as_ref() {
            PlanNode::ValuesNode { rows, .. } => {
                let blocks = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| match expr.as_ref() {
                                ConstantExpression { valueBlock, .. } => {
                                    BlockBuilder::try_from(&valueBlock.0)
                                }
                                _ => Err(anyhow!("unsupported expression type in Values operator")),
                            })
                            .collect::<Result<Vec<_>, Error>>()
                    })
                    .reduce(|accumulator, e| match accumulator {
                        Ok(mut blocks) => match e {
                            Ok(other_blocks) => {
                                let ok = blocks
                                    .iter_mut()
                                    .zip(other_blocks)
                                    .map(|(initial, other)| initial.extend(other))
                                    .collect::<Result<Vec<_>, _>>();
                                ok.map(|_| blocks)
                            }
                            err => err,
                        },
                        err => err,
                    });
                let blocks = blocks.unwrap_or(Ok(vec![]))?;
                // let blocks: Vec<_> = blocks.into_iter().map(BlockBuilder::build).collect();
                let mut pg = PageBuilder::new();
                for block in blocks {
                    if let Err(e) = pg.add_channel(block) {
                        warn!("failed to build page from values blocks: {:?}", e);
                    }
                }
                tokio::spawn(async move {
                    match new_post {
                        Some(chan) => {
                            if let Err(e) = chan.send(DriverMessage::Input(pg.build())).await {
                                warn!("ValuesNode failed to send to next op: {:?}", e);
                            }
                        }
                        None => warn!("sender channel was dropped"),
                    }
                });
                Ok(())
            }
            a => Err(anyhow!("SourceOperators not implemented for {:?}", a)),
        };
        match self.post_office.read().await.deref() {
            Some(chan) => {
                if let Err(e) = chan.send(DriverMessage::Finished).await {
                    warn!("source operator failed to send finished message: {:?}", e);
                }
            }
            None => warn!("failed to send driver finished because channel was closed"),
        }
        *self.post_office.write().await = None;
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

    pub async fn start(&self, output_buffer: UnboundedSender<SerializedPage>) -> Result<()> {
        debug!("executing plan: {:#?}", self.graph);
        if self.source_operator.is_none() {
            return Err(anyhow!("no source operator found for execution"));
        }
        let source_op = self.source_operator.as_ref().unwrap().clone();
        let start_handle = tokio::spawn(async move { source_op.start().await });
        let mut drivers = vec![];
        for driver in self.operators.iter() {
            if driver.as_source_operator().is_none() {
                let driver = driver.clone();
                drivers.push(tokio::spawn(async move { driver.run_operator().await }));
            }
        }

        loop {
            match self.mailbox.lock().await.recv().await {
                Some(DriverMessage::Finished) => {
                    debug!("Final execution should be done soon");
                }
                Some(DriverMessage::Input(page)) => match SerializedPage::try_from(page) {
                    Ok(page) => {
                        if let Err(e) = output_buffer.send(page) {
                            warn!("driver failed send page to output buffers: {:?}", e);
                        }
                    }
                    Err(e) => {
                        return Err(anyhow!("Failed to serialize page: {:?}", e));
                    }
                },
                Some(output) => warn!("output driver received unhandled message {:?}", output),
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
        Ok(())
    }
}
