pub mod impls;
pub mod resources;
include!(concat!(env!("OUT_DIR"), "/tz.rs"));

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
