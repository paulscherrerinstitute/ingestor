use std::io::ErrorKind;
use bsread::{IOError, IOResult, ScalarType};


pub fn kind_to_dtype(kind: ScalarType) -> i8 {
    match kind {
        ScalarType::bool => {0}
        ScalarType::int8 => {1}
        ScalarType::uint8 => {2}
        ScalarType::int16 => {3}
        ScalarType::uint16 => {4}
        ScalarType::int32 => {5}
        ScalarType::uint32 => {6}
        ScalarType::int64 => {7}
        ScalarType::uint64 => {8}
        ScalarType::float32 => {9}
        ScalarType::float64 => {10}
        ScalarType::string => {11}
    }
}

pub fn dtype_to_kind(dtype:u8) -> ScalarType {
    match dtype {
        0 => {ScalarType::bool}
        1 => {ScalarType::int8}
        2 => {ScalarType::uint8}
        3 => {ScalarType::int16}
        4 => {ScalarType::uint16}
        5 => {ScalarType::int32}
        6 => {ScalarType::uint32}
        7 => {ScalarType::int64}
        8 => {ScalarType::uint64}
        9 => {ScalarType::float32}
        10 => {ScalarType::float64}
        11 => {ScalarType::string}
        _ => {
            log::error!("Unknown dtype {}", dtype);
            ScalarType::float64
        }
    }
}

pub fn kind_to_cql_type(kind:ScalarType) -> &'static str {
    match kind {
        ScalarType::string  => "text",
        ScalarType::bool    => "boolean",
        ScalarType::int8    => "tinyint",
        ScalarType::uint8   => "smallint",
        ScalarType::int16   => "smallint",
        ScalarType::uint16  => "int",
        ScalarType::int32   => "int",
        ScalarType::uint32  => "bigint",
        ScalarType::int64   => "bigint",
        ScalarType::uint64  => "blob",
        ScalarType::float32 => "float",
        ScalarType::float64 => "double"
    }
}

pub fn decode<T, const N: usize>(data: Vec<u8>, f: impl FnOnce([u8; N]) -> T,) -> IOResult<T> {
    let bytes: [u8; N] = data.try_into()
        .map_err(|_| IOError::new(ErrorKind::InvalidData, "Invalid data size"))?;
    Ok(f(bytes))
}

pub fn encode_to_i64(kind: ScalarType, data: Option<Vec<u8>>) -> IOResult<Option<i64>> {
    let Some(data) = data else {
        return Ok(None);
    };

    let value = match kind {
        ScalarType::bool => {decode(data, |b: [u8; 1]| if b[0] != 0 { 1 } else { 0 })?}

        ScalarType::int8 => {
            decode(data, i8::from_le_bytes)? as i64
        }

        ScalarType::uint8 => {
            decode(data, u8::from_le_bytes)? as i64
        }

        ScalarType::int16 => {
            decode(data, i16::from_le_bytes)? as i64
        }

        ScalarType::uint16 => {
            decode(data, u16::from_le_bytes)? as i64
        }

        ScalarType::int32 => {
            decode(data, i32::from_le_bytes)? as i64
        }

        ScalarType::uint32 => {
            decode(data, u32::from_le_bytes)? as i64
        }

        ScalarType::int64 => {
            decode(data, i64::from_le_bytes)?
        }

        ScalarType::uint64 => {
            decode(data, u64::from_le_bytes)? as i64
        }

        ScalarType::float32 => {
            decode(data, f32::from_le_bytes)?.to_bits() as i64
        }

        ScalarType::float64 => {
            decode(data, f64::from_le_bytes)?.to_bits() as i64
        }

        ScalarType::string => {
            return Err(IOError::new(
                ErrorKind::InvalidInput,
                "String is not a scalar numeric type",
            ));
        }
    };
    Ok(Some(value))
}


pub fn decode_optional<T, const N: usize>(data: Option<Vec<u8>>,f: impl FnOnce([u8; N]) -> T,) -> IOResult<Option<T>> {
    data.map(|data| {
        let bytes: [u8; N] = data
            .try_into()
            .map_err(|_| IOError::new(ErrorKind::InvalidData, "Invalid data size"))?;
        Ok(f(bytes))
    })
    .transpose()
}

pub enum ScalarValue {
    String(String),
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Blob(Vec<u8>),
}

pub fn decode_scalar( kind: ScalarType, data: Option<Vec<u8>> ) -> IOResult<Option<ScalarValue>> {
    match kind {
        ScalarType::string => {
            data.map(|data| {
                String::from_utf8(data)
                    .map(ScalarValue::String)
                    .map_err(|e| IOError::new(ErrorKind::InvalidData, e))
            })
                .transpose()
        }

        ScalarType::bool => {
            decode_optional(data, |b: [u8; 1]| b[0] != 0)
                .map(|value| value.map(ScalarValue::Bool))
        }

        ScalarType::int8 => {
            decode_optional(data, i8::from_le_bytes)
                .map(|value| value.map(ScalarValue::I8))
        }

        ScalarType::uint8 => {
            decode_optional(data, u8::from_le_bytes)
                .map(|value| value.map(ScalarValue::U8))
        }

        ScalarType::int16 => {
            decode_optional(data, i16::from_le_bytes)
                .map(|value| value.map(ScalarValue::I16))
        }

        ScalarType::uint16 => {
            decode_optional(data, u16::from_le_bytes)
                .map(|value| value.map(ScalarValue::U16))
        }

        ScalarType::int32 => {
            decode_optional(data, i32::from_le_bytes)
                .map(|value| value.map(ScalarValue::I32))
        }

        ScalarType::uint32 => {
            decode_optional(data, u32::from_le_bytes)
                .map(|value| value.map(ScalarValue::U32))
        }

        ScalarType::int64 => {
            decode_optional(data, i64::from_le_bytes)
                .map(|value| value.map(ScalarValue::I64))
        }

        ScalarType::uint64 => {
            decode_optional(data, u64::from_le_bytes)
                .map(|value| value.map(ScalarValue::U64))
        }

        ScalarType::float32 => {
            decode_optional(data, f32::from_le_bytes)
                .map(|value| value.map(ScalarValue::F32))
        }

        ScalarType::float64 => {
            decode_optional(data, f64::from_le_bytes)
                .map(|value| value.map(ScalarValue::F64))
        }
    }
}