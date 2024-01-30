
pub trait Connector {

}


pub mod tpch {
    #[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
    #[typetag::serde(name = "tpch")]
    struct TpchTransactionHandle {}
    impl ConnectorTransactionHandle for TpchTransactionHandle {

    }
}

pub mod hive {
}