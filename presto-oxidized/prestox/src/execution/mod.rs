use std::{collections::HashMap, rc::Rc, sync::Arc, time::Duration};

use futures::Future;

use anyhow::{anyhow, Result};
use tokio::{
    sync::{Mutex, Semaphore, SemaphorePermit},
    time::timeout,
};

use crate::{
    protocol::resources::{Block, PlanNodeId, ScheduledSplit, Split, TaskSource},
    spi::ConnectorPageSource,
};

pub struct DriverContext;

#[allow(unused)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum DriverState {
    Alive,
    NeedDestruction,
    Destroyed,
}

#[allow(unused)]
pub struct Driver {
    ctx: DriverContext,
    active_operators: Vec<Rc<Box<dyn Operator>>>,
    all_operators: Vec<Rc<Box<dyn Operator>>>,
    source_operator: Option<Rc<Box<dyn Operator>>>,
    delete_operator: Option<Rc<Box<dyn Operator>>>,
    update_operator: Option<Rc<Box<dyn Operator>>>,
    pending_task_source_updates: Mutex<Option<TaskSource>>,
    current_task_source: Mutex<Option<TaskSource>>,
    revoking_operators: HashMap<Rc<Box<dyn Operator>>, Box<dyn Future<Output = ()>>>,
    driver_blocked: Option<Box<dyn Future<Output = ()>>>,
    current_split: Mutex<Option<Split>>,
    output_pages: Vec<Page>,
    state: Mutex<DriverState>,
    run_lock: tokio::sync::Semaphore,
}

#[allow(unused)]
impl Driver {
    fn new(ctx: DriverContext, operators: impl Iterator<Item = Box<dyn Operator>>) -> Result<Self> {
        let all_operators: Vec<Rc<Box<dyn Operator>>> = operators.map(Rc::new).collect();
        let mut source_operator: Option<Rc<Box<dyn Operator>>> = None;
        let delete_operator: Option<Rc<Box<dyn Operator>>> = None;
        let update_operator: Option<Rc<Box<dyn Operator>>> = None;
        for op in all_operators.iter() {
            if let Some(_operator) = op.as_source_operator() {
                match source_operator {
                    Some(_x) => return Err(anyhow!("there must be at most one source operator")),
                    None => source_operator = Some(op.clone()),
                }
            }

            if let Some(_operator) = op.as_delete_operator() {
                match source_operator {
                    Some(_x) => return Err(anyhow!("there must be at most one delete operator")),
                    None => source_operator = Some(op.clone()),
                }
            }
            if let Some(_operator) = op.as_source_operator() {
                match source_operator {
                    Some(_x) => return Err(anyhow!("there must be at most one update operator")),
                    None => source_operator = Some(op.clone()),
                }
            }
        }
        Ok(Driver {
            ctx,
            active_operators: all_operators.as_slice().to_vec(),
            all_operators,
            source_operator,
            delete_operator,
            update_operator,
            pending_task_source_updates: Mutex::new(None),
            revoking_operators: HashMap::new(),
            current_task_source: Mutex::new(None),
            current_split: Mutex::new(None),
            output_pages: Vec::new(),
            driver_blocked: None,
            run_lock: Semaphore::new(1),
            state: Mutex::new(DriverState::Alive),
        })
    }

    async fn process_for(&self, _duration: Duration) {}

    async fn process(&self) -> Result<()> {
        if self.check_lock_held() {
            return Err(anyhow!("Lock should not be held on ::process"));
        }
        Ok(())
    }

    fn check_lock_held(&self) -> bool {
        self.run_lock.available_permits() > 0
    }

    async fn process_new_sources<'b, 'a: 'b>(
        &'a self,
        permit: SemaphorePermit<'b>,
    ) -> (SemaphorePermit<'b>, Result<()>) {
        if *self.state.lock().await != DriverState::Alive {
            return (permit, Ok(()));
        }
        let mut source_update: Option<TaskSource> = None;
        {
            let mut source_task = self.pending_task_source_updates.lock().await;
            if source_task.is_none() {
                return (permit, Ok(()));
            }
            source_update = source_task.take();
        }

        {
            match *self.current_task_source.lock().await {
                Some(ref task) => {
                    if let Some(incoming) = source_update {
                        match task.update(&incoming) {
                            Some(updated_task) => {
                                let task_splits = task.get_splits();
                                let new_splits = updated_task.get_splits();
                                let _new_split_difference = new_splits.difference(&task_splits);
                                let source_op = self
                                    .source_operator
                                    .as_ref()
                                    .ok_or(anyhow!("must have source operator"));
                                if source_op.is_err() {
                                    return (permit, Err(source_op.err().unwrap()));
                                }
                                let source_op = source_op.ok().unwrap().as_source_operator();
                                if let None = source_op {
                                    return (permit, Err(anyhow!("source op was none!")));
                                }
                                let source_op = source_op.unwrap();

                                for new_split in new_splits {
                                    // add fragment result caching here
                                    let page_source = Arc::new(source_op.add_split(new_split));
                                    if let Some(delete_op) = &self.delete_operator {
                                        delete_op
                                            .as_delete_operator()
                                            .expect("should have been a delete operator")
                                            .set_page_source(page_source.clone())
                                    }
                                    if let Some(update_op) = &self.update_operator {
                                        update_op
                                            .as_update_operator()
                                            .expect("should have been an update operator")
                                            .set_page_source(page_source)
                                    }
                                }
                            }
                            None => return (permit, Ok(())),
                        }
                    }
                }
                None => {
                    return (permit, Ok(()));
                }
            }
        }

        return (permit, Ok(()));
    }

    async fn try_with_lock<T>(&self, lock_timeout: Duration, task: impl Fn() -> T) -> Option<T> {
        match timeout(lock_timeout, self.run_lock.acquire()).await {
            Ok(acquire_result) => match acquire_result {
                Ok(permit) => {
                    let task = Some(task());
                    let _permit = self.process_new_sources(permit).await;
                    return task;
                }
                Err(_) => None,
            },
            Err(_) => None,
        }
    }
}

#[allow(unused)]
pub struct Page {
    blocks: Vec<Block>,
    position_count: u32,
    size_in_bytes: u64,
    retained_size_in_bytes: u64,
    logical_size_in_bytes: u64,
}

pub struct OperatorContext;

pub trait Operator {
    fn get_operator_context(&self) -> OperatorContext;
    fn is_blocked(&self) -> Box<dyn Future<Output = Result<()>>> {
        Box::new(futures::future::ok(()))
    }
    fn needs_input(&self) -> bool;
    fn add_input(&self, page: Page);
    fn get_output(&self) -> Page;
    fn start_memory_revoke(&self) -> Box<dyn Future<Output = Result<()>>> {
        Box::new(futures::future::ok(()))
    }
    fn finish_memory_revoke(&self) {}
    fn finish(&self);
    fn is_finished(&self) -> bool;

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

pub trait SourceOperator: Operator {
    fn get_source_id(&self) -> PlanNodeId;
    fn add_split(
        &self,
        split: &ScheduledSplit,
    ) -> Box<dyn Fn() -> Option<Box<dyn ConnectorPageSource>>>;
    fn no_more_splits(&self);
    fn as_source_operator(&self) -> Option<&dyn SourceOperator>;
}
pub trait DeleteOperator: Operator {
    fn set_page_source(&self, source: Arc<Box<dyn Fn() -> Option<Box<dyn ConnectorPageSource>>>>);
}

pub trait UpdateOperator: Operator {
    fn set_page_source(&self, source: Arc<Box<dyn Fn() -> Option<Box<dyn ConnectorPageSource>>>>);
}



#[allow(unused)]
mod sql_type {
    enum ScalarType {
        Int,
        Double,
        Bigint,
    }
    
    enum ParametricType {
        Map(Box<Type>, Box<Type>),
        Array(Box<Type>),
        Row(Vec<NamedType>),
        QDigest(Box<Type>),
        TDigest(Box<Type>),
        KllSketch(Box<Type>),
        Varchar(u32),
        Char(u16),
    }
    
    struct NamedType {
        name: String,
        inner_type: Type,
    }
    
    enum Type {
        Scalar(ScalarType),
        Parametric(ParametricType),
    }

}
