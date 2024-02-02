use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::protocol::resources::{HostAddress, NodeSelectionStrategy};

pub trait ConnectorProvider: SplitProvider {
    fn name() -> String;
}

pub trait SplitProvider: ConnectorSplit {
    fn name() -> String;
    fn serialize() {}
}

pub trait ConnectorPageSource {}

pub trait ConnectorSplit: Serialize + for<'a> Deserialize<'a> {
    /**
     * Indicate the node affinity of a Split
     * 1. HARD_AFFINITY: Split is NOT remotely accessible and has to be on specific nodes
     * 2. SOFT_AFFINITY: Connector split provides a list of preferred nodes for engine to pick from but not mandatory.
     * 3. NO_PREFERENCE: Split is remotely accessible and can be on any nodes
     */
    fn get_node_selection_strategy() -> NodeSelectionStrategy;

    /**
     * Provide a list of preferred nodes for scheduler to pick.
     * 1. The scheduler will respect the preference if the strategy is HARD_AFFINITY.
     * 2. Otherwise, the scheduler will prioritize the provided nodes if the strategy is SOFT_AFFINITY.
     * But there is no guarantee that the scheduler will pick them if the provided nodes are busy.
     * 3. Empty list indicates no preference.
     */
    fn get_preferred_nodes(node_provider: &impl NodeProvider) -> Vec<HostAddress>;

    fn get_info_map() -> Option<HashMap<String, String>> {
        None
    }

    fn get_split_size_in_bytes() -> Option<i64> {
        None
    }

    fn get_split_weight() -> SplitWeight {
        SplitWeight::standard()
    }
}

pub trait NodeProvider {
    /**
     * @param identifier   an unique identifier used to obtain the nodes
     * @param count        how many desirable nodes to be returned
     * @return a list of the chosen nodes by specific hash function
     */
    fn get(identifier: &str, count: i32) -> Vec<HostAddress>;
}

#[allow(unused)]
pub struct SplitWeight(i64);

#[allow(unused)]
impl SplitWeight {
    const UNIT_VALUE: i64 = 100;
    const UNIT_SCALE: i64 = 2;
    const fn standard() -> Self {
        SplitWeight(SplitWeight::UNIT_VALUE)
    }

    fn from_proportion(weight: f64) -> SplitWeight {
        SplitWeight((weight * SplitWeight::UNIT_VALUE as f64).ceil() as i64)
    }
}
