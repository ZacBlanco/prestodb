#![allow(unused)]
use super::page::{BlockBuf, BlockBuilder};

#[derive(Debug, Clone)]
pub enum ScalarType {
    Int,
    Double,
    Bigint,
}

#[derive(Debug, Clone)]
pub enum ParametricType {
    Map(Box<SqlType>, Box<SqlType>),
    Array(Box<SqlType>),
    Row(Vec<NamedType>),
    QDigest(Box<SqlType>),
    TDigest(Box<SqlType>),
    KllSketch(Box<SqlType>),
    Varchar(u32),
    Char(u16),
}


#[derive(Debug, Clone)]
pub struct NamedType {
    name: String,
    inner_type: SqlType,
}

#[derive(Debug, Clone)]
pub enum SqlType {
    Scalar(ScalarType),
    Parametric(ParametricType),
}

impl SqlType {
    fn new_block_builder<T>(&self, initial_entries: usize) -> BlockBuilder<T>
    where
        T: BlockBuf,
    {
        BlockBuilder::new(self.clone(), initial_entries)
    }
}
