use metric_engine::{Metric, PersistentMetric};

/// Four-channel raw EEG sample from the external sensor stream.
#[derive(Clone, Debug)]
pub struct RawEegMetric {
    pub tp9: f32,
    pub af7: f32,
    pub af8: f32,
    pub tp10: f32,
}

impl Metric for RawEegMetric {
    fn id() -> &'static str {
        "raw_eeg"
    }
}

/// DuckDB schema and row conversion for raw EEG samples.
impl PersistentMetric for RawEegMetric {
    fn table_name() -> &'static str {
        "metrics_raw_eeg"
    }
    fn schema_columns() -> &'static [&'static str] {
        &["timestamp_ms", "tp9", "af7", "af8", "tp10"]
    }
    fn create_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS metrics_raw_eeg (timestamp_ms BIGINT PRIMARY KEY, tp9 REAL, af7 REAL, af8 REAL, tp10 REAL);"
    }
    fn insert_sql() -> &'static str {
        "INSERT INTO metrics_raw_eeg (timestamp_ms,tp9,af7,af8,tp10) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING;"
    }
    fn select_range_sql() -> &'static str {
        "SELECT timestamp_ms,tp9,af7,af8,tp10 FROM metrics_raw_eeg WHERE timestamp_ms >= ? AND timestamp_ms <= ? ORDER BY timestamp_ms ASC;"
    }
    fn to_sql_params(&self) -> Vec<String> {
        vec![
            self.tp9.to_string(),
            self.af7.to_string(),
            self.af8.to_string(),
            self.tp10.to_string(),
        ]
    }
    fn from_sql_row(row: &[&str]) -> Result<Self, String> {
        if row.len() < 4 {
            return Err("Invalid RawEegMetric row".into());
        }
        Ok(Self {
            tp9: row[0].parse::<f32>().map_err(|e| e.to_string())?,
            af7: row[1].parse::<f32>().map_err(|e| e.to_string())?,
            af8: row[2].parse::<f32>().map_err(|e| e.to_string())?,
            tp10: row[3].parse::<f32>().map_err(|e| e.to_string())?,
        })
    }
}
