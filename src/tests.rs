use crate::db::DB;
use crate::arguments::{Arguments, StorageLayout, StartupState};
use crate::codec::{decode_optional, decode_scalar, encode_to_i64, ScalarValue};
use crate::config::{Config, Source};
use crate::cql;

use bsread::ScalarType;
use std::path::PathBuf;
use std::sync::{Arc, Once};

static DB_KEYSPACE_INIT: Once = Once::new();

fn init_keyspace_once() {
    DB_KEYSPACE_INIT.call_once(|| {
        let arguments = Arc::new(Arguments {
            storage_layout: StorageLayout::Default,
            ..Arguments::default()
        });

        let _db = DB::new(arguments);
    });
}

fn unique_temp_path(name: &str) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after UNIX_EPOCH")
        .as_nanos();

    std::env::temp_dir().join(format!("ingestor_test_{pid}_{nanos}_{name}"))
}

#[test]
fn config_save_when_parent_directory_does_not_exist() {
    let path = unique_temp_path("missing_parent").join("config.json");

    let config = Config {
        sources: vec![Source {
            address: "tcp://localhost:12000".to_string(),
            socket_type: None,
            enabled: Some(true),
        }],
    };

    let result = config.save(&path);

    assert!(result.is_ok(),
        "Config::save should not report an error when the parent directory does not exist"
    );
}

#[test]
fn config_save_and_load_round_trip() {
    let dir = unique_temp_path("config_round_trip");
    std::fs::create_dir_all(&dir).expect("failed to create temp config directory");

    let path = dir.join("config.json");

    let config = Config {
        sources: vec![
            Source {
                address: "tcp://localhost:12000".to_string(),
                socket_type: None,
                enabled: Some(true),
            },
            Source {
                address: "tcp://localhost:12001".to_string(),
                socket_type: None,
                enabled: Some(false),
            },
        ],
    };

    config.save(&path).expect("config should save");
    let loaded = Config::load(&path).expect("config should load");

    assert_eq!(loaded.sources.len(), 2);
    assert_eq!(loaded.sources[0].address, "tcp://localhost:12000");
    assert_eq!(loaded.sources[0].enabled, Some(true));
    assert_eq!(loaded.sources[1].address, "tcp://localhost:12001");
    assert_eq!(loaded.sources[1].enabled, Some(false));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn metadata_fetch_query_uses_bind_marker_not_interpolated_channel_name() {
    init_keyspace_once();

    let query = cql::fetch_channel_metadata_query("channel_metadata");

    assert!(
        query.contains("WHERE channel_name = ?"),
        "metadata lookup should use a bind marker for channel name"
    );

    assert!(
        !query.contains("WHERE channel_name = '"),
        "metadata lookup must not interpolate channel names as quoted CQL literals"
    );
}

#[test]
fn fetch_channels_query_quotes_column_name_not_string_literal() {
    init_keyspace_once();

    let query = cql::fetch_channels_query("channel_metadata");

    assert!(
        query.contains("SELECT DISTINCT \"channel_name\""),
        "SELECT DISTINCT should quote the column identifier, not use a string literal"
    );

    assert!(
        !query.contains("SELECT DISTINCT channel_name\n"),
        "this assertion documents the intended explicit identifier quoting"
    );
}

#[test]
fn insert_data_query_uses_bind_markers_for_values() {
    init_keyspace_once();

    let query_without_type = cql::insert_data_query("measurements", false);
    assert!(
        query_without_type.contains("VALUES (?, ?, ?, ?)"),
        "insert without dtype column should bind all data values"
    );

    let query_with_type = cql::insert_data_query("measurements", true);
    assert!(
        query_with_type.contains("VALUES (?, ?, ?, ?, ?)"),
        "insert with dtype column should bind all data values"
    );
}

#[test]
fn create_data_table_escapes_double_quotes_in_table_name() {
    init_keyspace_once();

    let query = cql::create_data_table("bad\"table", "bigint", true);

    assert!(
        query.contains("\"bad\"\"table\""),
        "table identifiers containing double quotes should be escaped"
    );
}

#[test]
fn encode_to_i64_rejects_wrong_scalar_size() {
    let result = encode_to_i64(ScalarType::int32, Some(vec![1, 2, 3]));

    assert!(
        result.is_err(),
        "int32 values must contain exactly four bytes"
    );
}

#[test]
fn encode_to_i64_rejects_string_scalars() {
    let result = encode_to_i64(ScalarType::string, Some(b"hello".to_vec()));

    assert!(
        result.is_err(),
        "string data should not be encoded into the default i64 scalar representation"
    );
}

#[test]
fn encode_to_i64_preserves_float64_bits() {
    let value = 12.5_f64;
    let encoded = encode_to_i64(ScalarType::float64, Some(value.to_le_bytes().to_vec()))
        .expect("float64 should encode")
        .expect("non-null data should produce Some");

    assert_eq!(encoded, value.to_bits() as i64);
}

#[test]
fn decode_optional_returns_none_for_absent_data() {
    let decoded = decode_optional::<i32, 4>(None, i32::from_le_bytes)
        .expect("None should decode successfully");

    assert_eq!(decoded, None);
}

#[test]
fn decode_optional_rejects_wrong_size() {
    let decoded = decode_optional::<i32, 4>(Some(vec![1, 2, 3]), i32::from_le_bytes);

    assert!(
        decoded.is_err(),
        "decode_optional should reject data with the wrong byte length"
    );
}

#[test]
fn decode_scalar_decodes_utf8_string() {
    let decoded = decode_scalar(ScalarType::string, Some(b"channel-value".to_vec()))
        .expect("valid UTF-8 should decode");

    match decoded {
        Some(ScalarValue::String(value)) => assert_eq!(value, "channel-value"),
        other => panic!("expected decoded string, got unexpected value: {:?}", scalar_value_name(other)),
    }
}

#[test]
fn decode_scalar_rejects_invalid_utf8_string() {
    let decoded = decode_scalar(ScalarType::string, Some(vec![0xff, 0xfe, 0xfd]));

    assert!(
        decoded.is_err(),
        "invalid UTF-8 string payloads should be rejected"
    );
}

#[test]
fn decode_scalar_decodes_bool_zero_as_false() {
    let decoded = decode_scalar(ScalarType::bool, Some(vec![0]))
        .expect("bool should decode");

    match decoded {
        Some(ScalarValue::Bool(value)) => assert!(!value),
        other => panic!("expected decoded bool, got unexpected value: {:?}", scalar_value_name(other)),
    }
}

#[test]
fn decode_scalar_decodes_bool_nonzero_as_true() {
    let decoded = decode_scalar(ScalarType::bool, Some(vec![7]))
        .expect("bool should decode");

    match decoded {
        Some(ScalarValue::Bool(value)) => assert!(value),
        other => panic!("expected decoded bool, got unexpected value: {:?}", scalar_value_name(other)),
    }
}

#[test]
fn arguments_default_uses_safe_non_started_startup_state() {
    let arguments = Arguments::default();

    assert!(
        !arguments.start.is_started(),
        "default startup state should not immediately start external connections"
    );
}

#[test]
fn storage_layout_helpers_match_expected_layouts() {
    assert!(StorageLayout::Typed.is_typed());
    assert!(!StorageLayout::Default.is_typed());

    assert!(StorageLayout::Default.is_i64_encoded());
    assert!(!StorageLayout::Typed.is_i64_encoded());
}

fn scalar_value_name(value: Option<ScalarValue>) -> &'static str {
    match value {
        None => "None",
        Some(ScalarValue::String(_)) => "String",
        Some(ScalarValue::Bool(_)) => "Bool",
        Some(ScalarValue::I8(_)) => "I8",
        Some(ScalarValue::I16(_)) => "I16",
        Some(ScalarValue::I32(_)) => "I32",
        Some(ScalarValue::I64(_)) => "I64",
        Some(ScalarValue::U8(_)) => "U8",
        Some(ScalarValue::U16(_)) => "U16",
        Some(ScalarValue::U32(_)) => "U32",
        Some(ScalarValue::U64(_)) => "U64",
        Some(ScalarValue::F32(_)) => "F32",
        Some(ScalarValue::F64(_)) => "F64",
        Some(ScalarValue::Blob(_)) => "Blob",
    }
}
