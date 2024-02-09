#![allow(unused)]
use bytes::Bytes;

use super::page::{BlockBuf, BlockBuilder};

#[derive(Debug, Clone)]
pub enum ScalarType {
    Int,
    Double,
    Bigint,
}

pub enum RawType {
    Byte(u8),
    Short(u16),
    Int(u32),
    Long(u64),
    Bytes(Bytes),
}

// impl BlockBuf for RawType {
//     fn put_intoß(&self, buf: &mut bytes::BytesMut) {
//         match self {
//             RawType::Byte(_) => todo!(),
//             RawType::Short(_) => todo!(),
//             RawType::Int(_) => todo!(),
//             RawType::Long(_) => todo!(),
//             RawType::Bytes(_) => todo!(),
//         }
//     }
// }

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
    fn new_block_builder(&self, initial_entries: usize) -> BlockBuilder {
        BlockBuilder::new(self.clone(), initial_entries)
    }
}
