use crate::StorageBackend;

/// DuckDB adapter boundary. A production application can replace the simple
/// connection handle with duckdb::Connection without changing the engine.

pub struct DuckDbBackend {
    pub database_path: String,
    pub flushed_rows: usize,
}

impl DuckDbBackend {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            database_path: path.into(),
            flushed_rows: 0,
        }
    }
}

impl StorageBackend for DuckDbBackend {
    fn flush_batch(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: &[String],
    ) -> Result<(), String> {
        self.flushed_rows += rows.len();

        println!(
            "DuckDB {}: flushed {} rows into {} ({})",
            self.database_path,
            rows.len(),
            table,
            columns.join(",")
        );

        Ok(())
    }

    fn fetch_historic(&self, table: &str, window_ms: i64) -> Result<Vec<String>, String> {
        println!(
            "DuckDB {}: fetch {}ms from {}",
            self.database_path, window_ms, table
        );

        Ok(Vec::new())
    }
}
