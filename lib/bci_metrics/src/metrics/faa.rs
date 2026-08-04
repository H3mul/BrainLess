use metric_engine::{Metric, PersistentMetric};

/// Frontal alpha asymmetry derived from the AF7 and AF8 channels.
#[derive(Clone, Debug)]
pub struct FaaMetric {
    pub faa: f32,
}
impl Metric for FaaMetric {}

/// DuckDB schema and row conversion for frontal alpha asymmetry.
impl PersistentMetric for FaaMetric {
    fn table_name() -> &'static str {
        "metrics_faa"
    }
    fn schema_columns() -> &'static [&'static str] {
        &["timestamp_ms", "faa"]
    }
    fn create_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS metrics_faa (timestamp_ms BIGINT PRIMARY KEY, faa REAL);"
    }
    fn insert_sql() -> &'static str {
        "INSERT INTO metrics_faa (timestamp_ms,faa) VALUES (?,?) ON CONFLICT DO NOTHING;"
    }
    fn select_range_sql() -> &'static str {
        "SELECT timestamp_ms,faa FROM metrics_faa WHERE timestamp_ms >= ? AND timestamp_ms <= ? ORDER BY timestamp_ms ASC;"
    }
    fn to_sql_params(&self) -> Vec<String> {
        vec![self.faa.to_string()]
    }
    fn from_sql_row(row: &[&str]) -> Result<Self, String> {
        Ok(Self {
            faa: row
                .first()
                .ok_or("Invalid FaaMetric row")?
                .parse::<f32>()
                .map_err(|e| e.to_string())?,
        })
    }
}
