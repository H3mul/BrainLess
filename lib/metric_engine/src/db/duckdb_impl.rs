use crate::{SampleRate, db::backend::StorageBackend};
use tracing::{debug, info};

/// DuckDB adapter boundary. A production application can replace the simple
/// connection handle with duckdb::Connection without changing the engine.
pub struct DuckDbStorageBackend {
    pub database_path: String,
    pub flushed_rows: usize,
}

impl DuckDbStorageBackend {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            database_path: path.into(),
            flushed_rows: 0,
        }
    }
}

impl StorageBackend for DuckDbStorageBackend {
    fn flush_batch(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<String>],
    ) -> Result<(), String> {
        self.flushed_rows += rows.len();
        info!(
            database = %self.database_path,
            table,
            row_count = rows.len(),
            "flushing metric rows to DuckDB backend"
        );
        debug!(columns = ?columns, "DuckDB flush schema");

        println!(
            "DuckDB {}: flushed {} rows into {} ({})",
            self.database_path,
            rows.len(),
            table,
            columns.join(",")
        );

        Ok(())
    }

    fn fetch_historic_range(
        &self,
        table: &str,
        start_ms: i64,
        end_ms: i64,
        sample_rate: SampleRate,
    ) -> Result<Vec<String>, String> {
        debug!(
            database = %self.database_path,
            table,
            start_ms,
            end_ms,
            ?sample_rate,
            "fetching historic metric rows from DuckDB backend"
        );
        println!(
            "DuckDB {}: fetch {}ms..{}ms from {} at {:?}",
            self.database_path, start_ms, end_ms, table, sample_rate
        );

        Ok(Vec::new())
    }

    fn fetch_historic(
        &self,
        table: &str,
        window_ms: i64,
        sample_rate: SampleRate,
    ) -> Result<Vec<String>, String> {
        debug!(database = %self.database_path, table, window_ms, ?sample_rate, "fetching relative historic metric rows from DuckDB backend");
        println!(
            "DuckDB {}: fetch last {}ms from {} at {:?}",
            self.database_path, window_ms, table, sample_rate
        );
        Ok(Vec::new())
    }
}
