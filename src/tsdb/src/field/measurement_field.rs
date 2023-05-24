use common_base::influxql::DataType;
use dashmap::DashMap;
use influxdb_storage::StorageOperator;

/// Field represents a series field. All of the fields must be hashable.
pub struct Field {
    id: u8,
    name: String,
    r#type: DataType,
}

pub struct MeasurementFields {
    /// fields: map<field name, Field>
    fields: DashMap<String, Field>,
}

pub struct MeasurementFieldSet {
    op: StorageOperator,
    measure_fields: DashMap<String, MeasurementFields>,
}
