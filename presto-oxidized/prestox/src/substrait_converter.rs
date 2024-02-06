use base64::Engine;
use substrait::proto::{plan_rel::RelType, Plan, PlanRel, Rel, RelRoot};

use crate::{
    err::error::Error,
    protocol::resources::{Base64Encoded, PlanFragment, PlanNode, RowExpression},
};

use anyhow::Result;

impl TryFrom<&Base64Encoded> for PlanFragment {
    type Error = anyhow::Error;

    fn try_from(value: &Base64Encoded) -> Result<Self, Self::Error> {
        let mut result = Vec::new();
        if let Err(e) = base64::prelude::BASE64_STANDARD.decode_vec(&value.0, &mut result) {
            return Err(
                Error::PlanDecode(format!("base64 decode error: {:?}: {}", e, value.0)).into(),
            );
        }

        serde_json::from_slice(&result)
            .map_err(|e| Error::PlanDecode(format!("json decode error{:?}", e)).into())
    }
}

impl TryFrom<PlanFragment> for substrait::proto::Plan {
    type Error = anyhow::Error;
    fn try_from(value: PlanFragment) -> Result<Self, Self::Error> {
        Ok(Plan {
            version: None,
            extension_uris: vec![],
            extensions: vec![],
            relations: vec![value.try_into()?],
            advanced_extensions: None,
            expected_type_urls: vec![],
        })
    }
}

impl TryFrom<PlanFragment> for PlanRel {
    type Error = anyhow::Error;
    fn try_from(value: PlanFragment) -> Result<Self, Self::Error> {
        Ok(PlanRel {
            rel_type: Some(RelType::Root(RelRoot {
                names: value
                    .root
                    .get_output_variables()
                    .into_iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                input: (*value.root).try_into()?,
            })),
        })
    }
}

impl TryFrom<PlanNode> for Option<Rel> {
    type Error = anyhow::Error;
    fn try_from(value: PlanNode) -> Result<Self, Self::Error> {
        match value {
            PlanNode::EnforceSingleRowNode { id: _, source: _ } => todo!(),
            PlanNode::ExchangeNode {
                id: _,
                prestoType: _,
                scope: _,
                partitioningScheme: _,
                sources: _,
                inputs: _,
                ensureSourceOrdering: _,
                orderingScheme: _,
            } => todo!(),
            PlanNode::GroupIdNode {
                id: _,
                source: _,
                groupingSets: _,
                groupingColumns: _,
                aggregationArguments: _,
                groupIdVariable: _,
            } => todo!(),
            PlanNode::RowNumberNode {
                id: _,
                source: _,
                partitionBy: _,
                rowNumberVariable: _,
                maxRowCountPerPartition: _,
                partial: _,
                hashVariable: _,
            } => todo!(),
            PlanNode::SampleNode {
                id: _,
                source: _,
                sampleRatio: _,
                sampleType: _,
            } => todo!(),
            PlanNode::TopNRowNumberNode {
                id: _,
                source: _,
                specification: _,
                rowNumberVariable: _,
                maxRowCountPerPartition: _,
                partial: _,
                hashVariable: _,
            } => todo!(),
            PlanNode::TableWriterMergeNode {
                id: _,
                source: _,
                rowCountVariable: _,
                fragmentVariable: _,
                tableCommitContextVariable: _,
                statisticsAggregation: _,
            } => todo!(),
            PlanNode::UnnestNode {
                id: _,
                source: _,
                replicateVariables: _,
                unnestVariables: _,
                ordinalityVariable: _,
            } => todo!(),
            PlanNode::FilterNode {
                id: _,
                source: _,
                predicate: _,
            } => todo!(),
            PlanNode::LimitNode {
                id: _,
                source: _,
                count: _,
                step: _,
            } => todo!(),
            PlanNode::OutputNode {
                id: _,
                source: _,
                columnNames: _,
                outputVariables: _,
            } => todo!(),
            PlanNode::ProjectNode {
                id: _,
                source: _,
                assignments: _,
                locality: _,
            } => todo!(),
            PlanNode::RemoteSourceNode {
                id: _,
                sourceFragmentIds: _,
                outputVariables: _,
                ensureSourceOrdering: _,
                orderingScheme: _,
                exchangeType: _,
            } => todo!(),
            PlanNode::TableScanNode {
                id: _,
                table: _,
                outputVariables: _,
                assignments: _,
            } => todo!(),
            PlanNode::ValuesNode {
                location: _,
                id: _,
                outputVariables: _,
                rows: _,
                valuesNodeLabel: _,
            } => todo!(),
            PlanNode::SemiJoinNode {
                id: _,
                source: _,
                filteringSource: _,
                sourceJoinVariable: _,
                filteringSourceJoinVariable: _,
                semiJoinOutput: _,
                sourceHashVariable: _,
                filteringSourceHashVariable: _,
                distributionType: _,
                dynamicFilters: _,
            } => todo!(),
            PlanNode::MergeJoinNode {
                id: _,
                prestoType: _,
                left: _,
                right: _,
                criteria: _,
                outputVariables: _,
                filter: _,
                leftHashVariable: _,
                rightHashVariable: _,
            } => todo!(),
            PlanNode::TopNNode {
                id: _,
                source: _,
                count: _,
                orderingScheme: _,
                step: _,
            } => todo!(),
            PlanNode::SortNode {
                id: _,
                source: _,
                orderingScheme: _,
                isPartial: _,
            } => todo!(),
            PlanNode::AssignUniqueId {
                id: _,
                source: _,
                idVariable: _,
            } => todo!(),
            _ => Err(Error::Unimplemented.into()),
        }
    }
}

#[allow(unused)]
fn get_output_variables<'a>(
    input: impl Iterator<Item = &'a RowExpression>,
) -> Result<Vec<String>, Error> {
    input.map(|x| x.try_into().map_err(Error::from)).collect()
}

impl TryFrom<&RowExpression> for String {
    type Error = Error;

    fn try_from(value: &RowExpression) -> Result<Self, Self::Error> {
        serde_json::to_string(value).map_err(Error::from)
    }
}

#[cfg(test)]
mod test {
    use std::{fs::read_to_string, path::Path};

    use crate::protocol::resources::{Base64Encoded, PlanFragment};

    #[test]
    fn test_deserialize_fragment() {
        let resources = Path::new(env!("CARGO_MANIFEST_DIR"));
        let input = resources.join(Path::new("test-resources/test-serialized-fragment.txt"));
        let output = resources.join(Path::new(
            "test-resources/test-serialized-fragment-deserialized.json",
        ));
        let input: Base64Encoded = Base64Encoded(read_to_string(input).unwrap());
        let output = read_to_string(output).unwrap();
        let fragment = serde_json::from_str::<PlanFragment>(&output).unwrap();
        let plan: PlanFragment = (&input).try_into().unwrap();
        assert_eq!(plan, fragment);
    }
}
