use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    str::FromStr,
};

use anyhow::{anyhow, Error};
use base64::Engine;
use bitvec::prelude::BitVec;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{resources::Base64Encoded, sql_type::SqlType};

pub struct Page {
    #[allow(unused)]
    pub blocks: Vec<Block>,
    pub position_count: u32,
    pub size_in_bytes: u64,
    pub retained_size_in_bytes: u64,
    pub logical_size_in_bytes: u64,
}

impl Page {
    pub fn serialize(self) -> anyhow::Result<Bytes> {
        Bytes::try_from(SerializedPageRepr::try_from(self)?)
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("position_count", &self.position_count)
            .field("size_in_bytes", &self.size_in_bytes)
            .field("retained_size_in_bytes", &self.retained_size_in_bytes)
            .field("logical_size_in_bytes", &self.logical_size_in_bytes)
            .finish()
    }
}

/// Used in computations - should be cache aligned
#[repr(C, align(64))]
#[derive(Debug)]
pub struct Block {
    data: Bytes,
    encoding: BlockEncoding,
}

impl Default for Block {
    fn default() -> Self {
        Self {
            data: Default::default(),
            encoding: BlockEncoding::IntArray,
        }
    }
}

impl From<Block> for Bytes {
    fn from(value: Block) -> Self {
        // at minimum, block data size
        let encoding_name = value.encoding.as_str();
        let mut output = BytesMut::with_capacity(value.data.len() + encoding_name.len() + 4);
        output.put_u32_le(encoding_name.len() as u32);
        output.put_slice(encoding_name.as_bytes());
        output.extend_from_slice(value.data.as_ref());
        output.freeze()
    }
}

#[derive(Debug)]
pub enum BlockBuilder<T> {
    Array {
        entries: usize,
        nulls: BitVec<u8>,
        data: BytesMut,
        phantom: PhantomData<T>,
    },
}

pub trait BlockBuf {
    fn put_into(&self, buf: &mut BytesMut);
}

impl BlockBuf for Bytes {
    fn put_into(&self, buf: &mut BytesMut) {
        buf.put(&self[..])
    }
}

// impl BlockBuf for u32 {
//     fn put_into(&self, buf: &mut BytesMut) {
//         buf.put_u32_le(*self);
//     }
// }

macro_rules! blockbuf_primitive {
    ($implementor:ty) => {
        paste::paste! {
            impl BlockBuf for $implementor {
                fn put_into(&self, buf: &mut BytesMut) {
                    buf.[<put_ $implementor _le>](*self);
                }
            }
        }
    };
}

blockbuf_primitive!(i16);
blockbuf_primitive!(i32);
blockbuf_primitive!(i64);
blockbuf_primitive!(u16);
blockbuf_primitive!(u32);
blockbuf_primitive!(u64);

impl<T: BlockBuf> BlockBuilder<T> {
    pub fn new(sql_type: SqlType, initial_entries: usize) -> BlockBuilder<T> {
        match sql_type {
            SqlType::Scalar(_) => BlockBuilder::Array {
                entries: 0,
                nulls: BitVec::with_capacity(initial_entries),
                data: BytesMut::with_capacity(initial_entries),
                phantom: PhantomData,
            },
            SqlType::Parametric(_) => todo!(),
        }
    }

    pub fn append(&mut self, value: T) {
        match self {
            BlockBuilder::Array {
                entries,
                nulls,
                data,
                ..
            } => {
                *entries += 1;
                nulls.push(false);
                value.put_into(data)
            }
        }
    }

    pub fn append_null(&mut self) {
        match self {
            BlockBuilder::Array { entries, nulls, .. } => {
                *entries += 1;
                nulls.push(true)
            }
        }
    }

    /// returns the element at T, or None if null. Errors when index is out of range
    pub fn get(&self, index: usize) -> anyhow::Result<Option<&T>> {
        // first, calculate the index of the element
        match self {
            BlockBuilder::Array {
                entries,
                nulls,
                data,
                ..
            } => {
                if index > *entries {
                    return Err(anyhow!(
                        "block builder index out of range: {} > {}",
                        index,
                        entries
                    ));
                }
                match nulls[index] {
                    true => Ok(None),
                    false => {
                        let nulls_up_to_idx = nulls[0..index].count_ones();
                        let access_idx = index - nulls_up_to_idx;
                        unsafe {
                            let ptr: *const T = data.as_ptr().cast::<T>().add(access_idx);
                            let item = &*ptr;
                            Ok(Some(item))
                        }
                    }
                }
            }
        }
    }

    pub fn build(self) -> Block {
        match self {
            BlockBuilder::Array {
                entries,
                mut nulls,
                data,
                ..
            } => {
                let mut output = BytesMut::with_capacity(data.len() + 4 + nulls.len());
                output.put_u32_le(entries as u32);
                let has_nulls = nulls.count_ones() > 0;
                output.put_u8(if has_nulls { 1 } else { 0 });
                if has_nulls {
                    nulls.set_uninitialized(false);
                    output.put_slice(nulls.into_vec().as_slice())
                }

                Block {
                    data: output.freeze(),
                    encoding: BlockEncoding::IntArray,
                }
            }
        }
    }
}

impl<T: BlockBuf> TryFrom<Base64Encoded> for BlockBuilder<T> {
    type Error = Error;

    fn try_from(value: Base64Encoded) -> Result<Self, Self::Error> {
        let decoded = base64::prelude::BASE64_STANDARD.decode(value.0)?;
        BlockBuilder::try_from(Bytes::from(decoded))
    }
}

impl<T: BlockBuf> TryFrom<Bytes> for BlockBuilder<T> {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut value = value;
        let encoding_size = value.get_u32_le();
        let raw_encoding = value.slice(0..encoding_size as usize);
        let encoding_name = std::str::from_utf8(&raw_encoding)?;
        let encoding = BlockEncoding::from_str(encoding_name)?;
        value.advance(raw_encoding.len());
        encoding.deserialize_from(value)
    }
}

struct SerializedPageRepr {
    header: SerializedPageHeaderRepr,
    columns: u32,
    /// blocks should always be equivalent in length to columns
    blocks: Vec<SerializedBlock>,
}

impl TryFrom<SerializedPageRepr> for Bytes {
    type Error = Error;

    fn try_from(value: SerializedPageRepr) -> Result<Self, Self::Error> {
        let mut output = BytesMut::with_capacity(value.header.size as usize);
        output.extend_from_slice(Bytes::from(value.header).as_ref());
        output.put_u32_le(value.columns);
        value
            .blocks
            .into_iter()
            .map(Bytes::from)
            .for_each(|x| output.extend_from_slice(x.as_ref()));
        Ok(output.freeze())
    }
}

impl TryFrom<Page> for SerializedPageRepr {
    type Error = Error;
    fn try_from(value: Page) -> Result<Self, Self::Error> {
        Ok(SerializedPageRepr {
            header: SerializedPageHeaderRepr {
                rows: value.position_count,
                codec: 0x0,
                uncompressed_size: value.size_in_bytes as u32,
                size: value.size_in_bytes as u32,
                checksum: 0x0,
            },
            columns: value.blocks.len() as u32,
            blocks: value
                .blocks
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<Block> for SerializedBlock {
    type Error = Error;
    fn try_from(value: Block) -> Result<Self, Self::Error> {
        Ok(SerializedBlock {
            encoding_name_length: value.encoding.to_string().len() as u32,
            encoding: value.encoding,
            data: value.data,
        })
    }
}

#[repr(C)]
struct SerializedPageHeaderRepr {
    rows: u32,
    codec: u8,
    uncompressed_size: u32,
    size: u32,
    checksum: u64,
}

impl From<SerializedPageHeaderRepr> for Bytes {
    fn from(value: SerializedPageHeaderRepr) -> Self {
        let mut output = BytesMut::with_capacity(24);
        output.put_u32_le(value.rows);
        output.put_u8(value.codec);
        output.put_u32_le(value.uncompressed_size);
        output.put_u32_le(value.size);
        output.put_u64_le(value.checksum);
        output.freeze()
    }
}

#[repr(C)]
struct SerializedBlock {
    encoding_name_length: u32,
    /// length should be equal to header_size field
    encoding: BlockEncoding,
    data: Bytes,
}

impl From<SerializedBlock> for Bytes {
    fn from(value: SerializedBlock) -> Self {
        // at minimum, block data size
        let encoding_name = value.encoding.as_str();
        let mut output = BytesMut::with_capacity(value.data.len() + encoding_name.len() + 4);
        output.put_u32_le(encoding_name.len() as u32);
        output.put_slice(encoding_name.as_bytes());
        output.extend_from_slice(value.data.as_ref());
        output.freeze()
    }
}

// enum SerializedBlock {
//     ArrayType {
//         /// not stored in the serialized form
//         bytes_per_value: u32,
//         number_of_rows: u32,
//         has_nulls: Option<bool>,
//         nulls_bitflags: Bytes,
//         data: Bytes,
//     },
// }

#[allow(unused)]
#[derive(Debug)]
enum BlockEncoding {
    ByteArray,
    ShortArray,
    IntArray,
    LongArray,
    Int128Array,
    VariableWidth,
    Array,
    Map,
    MapElement,
    Row,
    Dictionary,
    Rle,
}

impl BlockEncoding {
    #[allow(unused)]
    fn width(&self) -> Option<u32> {
        match self {
            BlockEncoding::ByteArray => Some(std::mem::size_of::<u8>()),
            BlockEncoding::ShortArray => Some(std::mem::size_of::<u16>()),
            BlockEncoding::IntArray => Some(std::mem::size_of::<u32>()),
            BlockEncoding::LongArray => Some(std::mem::size_of::<u64>()),
            _ => None,
        }
        .map(|size| size as u32)
    }

    fn as_str(&self) -> &'static str {
        match self {
            BlockEncoding::ByteArray => "BYTE_ARRAY",
            BlockEncoding::ShortArray => "SHORT_ARRAY",
            BlockEncoding::IntArray => "INT_ARRAY",
            BlockEncoding::LongArray => "LONG_ARRAY",
            _ => "UNINIMPLEMENTED",
        }
    }

    fn deserialize_from<T>(&self, mut bytes: Bytes) -> anyhow::Result<BlockBuilder<T>> {
        match self {
            Self::ByteArray
            | Self::ShortArray
            | Self::IntArray
            | Self::LongArray
            | Self::Int128Array => {
                let rows = bytes.get_u32_le();
                let nulls_bytes = u32::div_ceil(rows, 8);
                let has_nulls = bytes.get_u8() == 1;
                let null_flags = if has_nulls {
                    BitVec::from_slice(bytes.slice(0..nulls_bytes as usize).as_ref())
                } else {
                    BitVec::repeat(false, rows as usize)
                };
                let mut copied = BytesMut::with_capacity(bytes.remaining());
                copied.put(bytes.chunk());
                Ok(BlockBuilder::Array {
                    entries: rows as usize,
                    nulls: null_flags,
                    data: copied,
                    phantom: PhantomData,
                })
            }
            encoding => Err(anyhow!(
                "BlockBuilder::from_bytes not implemented for {:?} block encoding",
                encoding
            )),
        }
    }
}

impl FromStr for BlockEncoding {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BYTE_ARRAY" => Ok(BlockEncoding::ByteArray),
            "SHORT_ARRAY" => Ok(BlockEncoding::ShortArray),
            "INT_ARRAY" => Ok(BlockEncoding::IntArray),
            "LONG_ARRAY" => Ok(BlockEncoding::LongArray),
            encoding => Err(anyhow!("Unsupported encoding type! {}", encoding)),
        }
    }
}

impl Display for BlockEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod test {
    use crate::protocol::{
        resources::Base64Encoded,
        sql_type::{ScalarType, SqlType},
    };

    use super::BlockBuilder;

    #[test]
    fn test_deserialize_block() -> anyhow::Result<()> {
        let serialized_block = Base64Encoded("CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==".to_string());
        let mut block: BlockBuilder<u32> = BlockBuilder::try_from(serialized_block)?;
        assert!(block.get(0).is_ok());
        assert!(block.get(0).unwrap().is_some());
        assert_eq!(*block.get(0).unwrap().unwrap(), 1u32);
        block.append_null();
        block.append_null();
        assert!(block.get(1).is_ok());
        assert!(block.get(1).unwrap().is_none());
        assert!(block.get(2).is_ok());
        assert!(block.get(2).unwrap().is_none());
        block.append(17u32);
        assert!(block.get(3).is_ok());
        assert!(block.get(3).unwrap().is_some());
        assert_eq!(*block.get(3).unwrap().unwrap(), 17u32);
        let _block = block.build();
        Ok(())
    }

    macro_rules ! test_block_type {
        ($implementor:ty) => {
            paste::item! {
                #[test]
                pub fn [<test_block_builder_ $implementor>]() {
                    let mut block: BlockBuilder<$implementor> = BlockBuilder::new(SqlType::Scalar(ScalarType::Int), 10);
                    block.append(12 as $implementor);
                    block.append_null();
                    block.append_null();
                    block.append_null();
                    block.append(15 as $implementor);
                    assert_eq!(*block.get(0).unwrap().unwrap(), 12 as $implementor);
                    assert!(block.get(1).unwrap().is_none());
                    assert!(block.get(2).unwrap().is_none());
                    assert!(block.get(3).unwrap().is_none());
                    assert_eq!(*block.get(4).unwrap().unwrap(), 15 as $implementor);
                }
            }
        }
    }

    test_block_type!(i16);
    test_block_type!(i32);
    test_block_type!(i64);
    test_block_type!(u16);
    test_block_type!(u32);
    test_block_type!(u64);
}
