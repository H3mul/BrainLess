use metric_engine::{Metric, TsdbStorage};

#[derive(Clone, Debug)]
pub struct FaaVolatilityMetric {
    pub volatility: f32,
}
impl Metric for FaaVolatilityMetric {}

impl TsdbStorage for FaaVolatilityMetric {
    fn table_name() -> &'static str {
        "metrics_faa_volatility"
    }
    fn schema_columns() -> &'static [&'static str] {
        &["timestamp_ms", "volatility"]
    }
    fn create_table_sql() -> &'static str {
        "CREATE TABLE IF NOT EXISTS metrics_faa_volatility (timestamp_ms BIGINT PRIMARY KEY, volatility REAL);"
    }
    fn insert_sql() -> &'static str {
        "INSERT INTO metrics_faa_volatility (timestamp_ms,volatility) VALUES (?,?) ON CONFLICT DO NOTHING;"
    }
    fn select_range_sql() -> &'static str {
        "SELECT timestamp_ms,volatility FROM metrics_faa_volatility WHERE timestamp_ms >= ? AND timestamp_ms <= ? ORDER BY timestamp_ms ASC;"
    }
    fn to_sql_params(&self) -> Vec<String> {
        vec![self.volatility.to_string()]
    }
    fn from_sql_row(row: &[&str]) -> Result<Self, String> {
        Ok(Self {
            volatility: row
                .first()
                .ok_or("Invalid volatility row")?
                .parse::<f32>()
                .map_err(|e| e.to_string())?,
        })
    }
}
