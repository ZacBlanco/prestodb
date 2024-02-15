use std::collections::{BTreeMap, HashSet};

use super::resources::{
    Assignments, BufferState, ConnectorId, DataSize, DateTime, Lifespan, MetadataUpdates,
    OutputBufferInfo, PlanNode, QualifiedObjectName, RowExpression, RuntimeStats, ScheduledSplit,
    TaskId, TaskSource, TaskStats,
};
use chrono::Utc;
use log::warn;
use regex::Regex;
use serde::{de::Visitor, Deserialize, Serialize};

impl From<&TaskId> for String {
    fn from(value: &TaskId) -> Self {
        value.to_string()
    }
}

impl Serialize for TaskId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TaskId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TaskIdVisitor)
    }
}

impl TaskSource {
    pub fn update(&self, _other: &TaskSource) -> Option<TaskSource> {
        None
    }

    pub fn get_splits<'b, 'a: 'b>(&'a self) -> HashSet<&'a ScheduledSplit> {
        HashSet::new()
    }
}

struct TaskIdVisitor;
impl<'de> Visitor<'de> for TaskIdVisitor {
    type Value = TaskId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Expecting a string in the format {}.{}.{}.{}.{}")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let parts = v.split('.').collect::<Vec<_>>();
        if parts.len() != 5 {
            return Err(E::custom(format!(
                "TaskId Should have had 5 parts. Only found {}",
                parts.len()
            )));
        }
        Ok(TaskId {
            query_id: parts[0].to_string(),
            stage_id: parts[1]
                .parse::<i32>()
                .map_err(|_| E::custom("integer parse failed on [1]"))?,
            stage_execution_id: parts[2]
                .parse::<i32>()
                .map_err(|_| E::custom("integer parse failed on [2]"))?,
            id: parts[3]
                .parse::<i32>()
                .map_err(|_| E::custom("integer parse failed on [3]"))?,
            attempt_number: parts[4]
                .parse::<i32>()
                .map_err(|_| E::custom("integer parse failed on [4]"))?,
        })
    }
}

impl Default for ConnectorId {
    fn default() -> Self {
        Self("tpch".to_string())
    }
}

#[allow(clippy::derivable_impls)]
impl Default for MetadataUpdates {
    fn default() -> Self {
        Self {
            connectorId: Default::default(),
            metadataUpdates: Default::default(),
        }
    }
}

impl Default for OutputBufferInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputBufferInfo {
    pub fn new() -> Self {
        OutputBufferInfo {
            prestoType: "varchar".to_string(),
            state: BufferState::OPEN,
            canAddBuffers: true,
            canAddPages: true,
            totalBufferedBytes: 1024 * 1024,
            totalBufferedPages: 100,
            totalRowsSent: 100000,
            totalPagesSent: 100,
            buffers: vec![],
        }
    }
}

impl Default for TaskStats {
    fn default() -> Self {
        TaskStats {
            createTime: DateTime(Utc::now()),
            firstStartTime: DateTime(Utc::now()),
            lastStartTime: DateTime(Utc::now()),
            lastEndTime: DateTime(Utc::now()),
            endTime: DateTime(Utc::now()),
            elapsedTimeInNanos: Default::default(),
            queuedTimeInNanos: Default::default(),
            totalDrivers: Default::default(),
            queuedDrivers: Default::default(),
            queuedPartitionedDrivers: Default::default(),
            queuedPartitionedSplitsWeight: Default::default(),
            runningDrivers: Default::default(),
            runningPartitionedDrivers: Default::default(),
            runningPartitionedSplitsWeight: Default::default(),
            blockedDrivers: Default::default(),
            completedDrivers: Default::default(),
            cumulativeUserMemory: Default::default(),
            cumulativeTotalMemory: Default::default(),
            userMemoryReservationInBytes: Default::default(),
            revocableMemoryReservationInBytes: Default::default(),
            systemMemoryReservationInBytes: Default::default(),
            peakTotalMemoryInBytes: Default::default(),
            peakUserMemoryInBytes: Default::default(),
            peakNodeTotalMemoryInBytes: Default::default(),
            totalScheduledTimeInNanos: Default::default(),
            totalCpuTimeInNanos: Default::default(),
            totalBlockedTimeInNanos: Default::default(),
            fullyBlocked: Default::default(),
            blockedReasons: Default::default(),
            totalAllocationInBytes: Default::default(),
            rawInputDataSizeInBytes: Default::default(),
            rawInputPositions: Default::default(),
            processedInputDataSizeInBytes: Default::default(),
            processedInputPositions: Default::default(),
            outputDataSizeInBytes: Default::default(),
            outputPositions: Default::default(),
            physicalWrittenDataSizeInBytes: Default::default(),
            fullGcCount: Default::default(),
            fullGcTimeInMillis: Default::default(),
            pipelines: Default::default(),
            runtimeStats: RuntimeStats(BTreeMap::new()),
        }
    }
}

impl Serialize for DataSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}B", self.0))
    }
}

impl<'de> Deserialize<'de> for DataSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(DataSizeVisitor)
    }
}

struct DataSizeVisitor;
impl<'de> Visitor<'de> for DataSizeVisitor {
    type Value = DataSize;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("expected a string to deserialize datasize")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let pattern = Regex::new(r"^\\s*(\\d+(?:\\.\\d+)?)\\s*([a-zA-Z]+)\\s*$").unwrap();
        if let Some(captures) = pattern.captures(v) {
            let (_, [size, unit]) = captures.extract();
            let size = size
                .parse::<f64>()
                .map_err(|_e| E::custom("size couldn't parse DataSize size"))?;
            let multiplier: u64 = match unit {
                "B" => Ok(1),
                "kB" => Ok(1 << 10),
                "MB" => Ok(1 << 20),
                "GB" => Ok(1 << 30),
                "TB" => Ok(1 << 40),
                "PB" => Ok(1 << 50),
                _ => Err(E::custom(format!("unknown unit size: {}", unit))),
            }?;
            return Ok(DataSize((size * multiplier as f64) as i64));
        }
        Err(E::custom(format!(
            "size is not a valid data size string: {}",
            v
        )))
    }
}

impl Lifespan {
    pub fn is_task_wide(&self) -> bool {
        !self.0 && self.1 == 0
    }

    pub fn task_wide() -> Self {
        Lifespan(false, 0)
    }
}

impl Serialize for Lifespan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&match self.is_task_wide() {
            true => "TaskWide".to_string(),
            false => format!("Group{}", self.1),
        })
    }
}

impl<'de> Deserialize<'de> for Lifespan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(LifespanVisitor)
    }
}

struct LifespanVisitor;

impl<'de> Visitor<'de> for LifespanVisitor {
    type Value = Lifespan;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("expected a string starting with 'Group' or 'TaskWide'")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.starts_with("TaskWide") {
            return Ok(Lifespan::task_wide());
        }

        match v.strip_prefix("Group") {
            Some(rest) => rest
                .parse::<i32>()
                .map_err(|e| E::custom(e.to_string()))
                .map(|group| Lifespan(true, group)),
            None => Err(E::custom(format!(
                "Looking for string starting with Group or TaskWide: {}",
                v
            ))),
        }
    }
}

impl PlanNode {
    pub fn get_output_variables(&self) -> Vec<&RowExpression> {
        match self {
            PlanNode::DistinctLimitNode {
                id: _,
                source: _,
                limit: _,
                partial: _,
                distinctVariables,
                hashVariable,
                timeoutMillis: _,
            } => {
                let mut outputs = vec![];
                for item in distinctVariables.iter() {
                    outputs.push(item);
                }
                if let Some(hashvar) = hashVariable {
                    outputs.push(hashvar);
                }
                outputs
            }
            PlanNode::ExchangeNode {
                id: _,
                prestoType: _,
                scope: _,
                partitioningScheme,
                sources: _,
                inputs: _,
                ensureSourceOrdering: _,
                orderingScheme: _,
            } => partitioningScheme.outputLayout.iter().collect(),
            PlanNode::GroupIdNode {
                id: _,
                source: _,
                groupingSets,
                groupingColumns: _,
                aggregationArguments,
                groupIdVariable,
            } => {
                let mut groupsets = HashSet::new();
                groupingSets.iter().for_each(|set| {
                    set.iter().for_each(|x| {
                        groupsets.insert(x);
                    })
                });
                let mut result = vec![];
                result.append(&mut groupsets.iter().copied().collect());
                result.append(&mut aggregationArguments.iter().collect());
                result.push(groupIdVariable);
                result
            }
            PlanNode::RowNumberNode {
                id: _,
                source,
                partitionBy: _,
                rowNumberVariable,
                maxRowCountPerPartition: _,
                partial,
                hashVariable: _,
            } => {
                let mut result = source.get_output_variables();
                if !partial {
                    result.push(rowNumberVariable);
                }
                result
            }
            PlanNode::TopNRowNumberNode {
                id: _,
                source,
                specification: _,
                rowNumberVariable,
                maxRowCountPerPartition: _,
                partial,
                hashVariable: _,
            } => {
                let mut result = source.get_output_variables();
                if !partial {
                    result.push(rowNumberVariable);
                }
                result
            }
            PlanNode::TableWriterMergeNode {
                id: _,
                source: _,
                rowCountVariable,
                fragmentVariable,
                tableCommitContextVariable,
                statisticsAggregation,
            } => {
                let mut result = vec![];
                result.push(rowCountVariable);
                result.push(fragmentVariable);
                result.push(tableCommitContextVariable);
                if let Some(agg) = statisticsAggregation {
                    let vars = agg.groupingVariables.iter();
                    result.append(&mut vars.collect());
                    result.append(&mut agg.aggregations.keys().collect());
                }
                result
            }
            PlanNode::UnnestNode {
                id: _,
                source: _,
                replicateVariables,
                unnestVariables,
                ordinalityVariable,
            } => {
                let mut result = vec![];
                result.append(&mut replicateVariables.iter().collect());
                result.append(&mut unnestVariables.values().flatten().collect());
                if let Some(var) = ordinalityVariable {
                    result.push(var);
                }
                result
            }
            PlanNode::OutputNode {
                id: _,
                source: _,
                columnNames: _,
                outputVariables,
            } => outputVariables.iter().collect(),
            PlanNode::ProjectNode {
                id: _,
                source: _,
                assignments,
                locality: _,
            } => assignments.get_outputs(),
            PlanNode::RemoteSourceNode {
                id: _,
                sourceFragmentIds: _,
                outputVariables,
                ensureSourceOrdering: _,
                orderingScheme: _,
                exchangeType: _,
            } => outputVariables.iter().collect(),
            PlanNode::TableScanNode {
                id: _,
                table: _,
                outputVariables,
                assignments: _,
            } => outputVariables.iter().collect(),
            PlanNode::ValuesNode {
                location: _,
                id: _,
                outputVariables,
                rows: _,
                valuesNodeLabel: _,
            } => outputVariables.iter().collect(),
            PlanNode::SemiJoinNode {
                id: _,
                source,
                filteringSource: _,
                sourceJoinVariable: _,
                filteringSourceJoinVariable: _,
                semiJoinOutput,
                sourceHashVariable: _,
                filteringSourceHashVariable: _,
                distributionType: _,
                dynamicFilters: _,
            } => {
                let mut result = source.get_output_variables();
                result.push(semiJoinOutput);
                result
            }
            PlanNode::MergeJoinNode {
                id: _,
                prestoType: _,
                left: _,
                right: _,
                criteria: _,
                outputVariables,
                filter: _,
                leftHashVariable: _,
                rightHashVariable: _,
            } => outputVariables.iter().collect(),
            PlanNode::AssignUniqueId {
                id: _,
                source,
                idVariable,
            } => {
                let mut result = source.get_output_variables();
                result.push(idVariable);
                result
            }
            _ => todo!(),
        }
    }

    pub fn get_sources(&self) -> Option<Vec<&PlanNode>> {
        match self {
            Self::OutputNode {
                id: _,
                source,
                columnNames: _,
                outputVariables: _,
            } => Some(vec![source]),
            Self::TableScanNode {
                id: _,
                table: _,
                outputVariables: _,
                assignments: _,
            } => None,
            Self::FilterNode {
                id: _,
                source,
                predicate: _,
            } => Some(vec![source]),
            Self::ProjectNode {
                id: _,
                source,
                assignments: _,
                locality: _,
            } => Some(vec![source]),
            Self::ValuesNode {
                location: _,
                id: _,
                outputVariables: _,
                rows: _,
                valuesNodeLabel: _,
            } => None,
            Self::LimitNode {
                id: _,
                source,
                count: _,
                step: _,
            } => Some(vec![source]),
            x => {
                warn!("get_sources not implemented for {:?}", x);
                todo!()
            }
        }
    }
}

impl Assignments {
    fn get_outputs(&self) -> Vec<&RowExpression> {
        self.assignments.keys().collect()
    }
}

impl Serialize for QualifiedObjectName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(
            format!(
                "{}.{}.{}",
                self.catalog_name, self.schema_name, self.object_name
            )
            .as_str(),
        )
    }
}

impl<'de> Deserialize<'de> for QualifiedObjectName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(QualifiedObjectNameDeserializer)
    }
}

struct QualifiedObjectNameDeserializer;

impl<'de> Visitor<'de> for QualifiedObjectNameDeserializer {
    type Value = QualifiedObjectName;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("expected a \".\" separated string with 3 parts")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let parts = v.split(".").collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(E::custom("3 parts . delimited string expected"));
        }
        Ok(QualifiedObjectName {
            catalog_name: parts[0].to_string(),
            schema_name: parts[1].to_string(),
            object_name: parts[2].to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::protocol::resources::{
        Base64Encoded, Block, ConnectorId, Lifespan, NodeSelectionStrategy, QualifiedObjectName,
        TaskId, Type, ValueEntry,
    };

    #[test]
    fn test_task_id_serialization() {
        let task = TaskId {
            query_id: "abc123".to_string(),
            stage_id: 5,
            stage_execution_id: 4,
            id: 3,
            attempt_number: 2,
        };
        let serialized = serde_json::to_string(&task).unwrap();
        assert_eq!(
            format!(
                "\"{}.{}.{}.{}.{}\"",
                task.query_id, task.stage_id, task.stage_execution_id, task.id, task.attempt_number
            ),
            serialized
        );
        let deser = serde_json::from_str::<TaskId>(&serialized).unwrap();
        assert_eq!(deser, task);
    }

    #[test]
    fn test_connector_id_serialization() {
        let id = ConnectorId("123".to_string());
        assert_eq!("\"123\"", serde_json::to_string(&id).unwrap());
        let new_id = serde_json::from_str(&serde_json::to_string(&id).unwrap()).unwrap();
        assert_eq!(id, new_id);
    }

    #[test]
    fn test_lifespan_serialization() {
        let span = Lifespan::task_wide();
        assert!(span.is_task_wide());
        assert_eq!("\"TaskWide\"", &serde_json::to_string(&span).unwrap());
        let span = Lifespan(true, 12);
        assert!(!span.is_task_wide());
        assert_eq!("\"Group12\"", &serde_json::to_string(&span).unwrap());
        assert_eq!(
            Lifespan::task_wide(),
            serde_json::from_str(&serde_json::to_string(&Lifespan::task_wide()).unwrap()).unwrap()
        );
        assert_eq!(
            Lifespan(true, 100),
            serde_json::from_str(&serde_json::to_string(&Lifespan(true, 100)).unwrap()).unwrap()
        );
    }

    #[test]
    fn test_qualified_object_name() {
        let obj = QualifiedObjectName {
            catalog_name: "catalog".to_string(),
            schema_name: "schema".to_string(),
            object_name: "$something".to_string(),
        };
        let result =
            serde_json::from_str::<QualifiedObjectName>(&serde_json::to_string(&obj).unwrap())
                .unwrap();
        assert_eq!(obj.catalog_name, result.catalog_name);
        assert_eq!(obj.schema_name, result.schema_name);
        assert_eq!(obj.object_name, result.object_name);
    }

    // this is just to test how the serde handles complex non-string types.
    // (hint: it doesn't)
    #[ignore]
    #[test]
    fn test_map_serialization() {
        let mut map = BTreeMap::new();
        map.insert(
            ValueEntry {
                prestoType: Type("test".to_string()),
                block: Block(Base64Encoded("abc123".to_string())),
            },
            NodeSelectionStrategy::HARD_AFFINITY,
        );
        println!("{}", serde_json::to_string_pretty(&map).unwrap());
    }
}
