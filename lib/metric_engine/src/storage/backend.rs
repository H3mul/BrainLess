/// Persistence boundary implemented by an application or database adapter.
///
/// The engine supplies table metadata and pre-formatted rows; the backend owns
/// connections, transactions, retries, and historic data access.
pub trait StorageBackend: Send + Sync {
    /// Writes a batch of rows to a metric table.
    fn flush_batch(
        &mut self,
        table_name: &str,
        schema_columns: &[&str],
        row_values: &[String],
    ) -> Result<(), String>;

    /// Fetches historic rows needed to initialize metric dependencies.
    fn fetch_historic(&self, table_name: &str, window_ms: i64) -> Result<Vec<String>, String>;
}

#[derive(Default)]

pub struct NoopStorageBackend;

impl StorageBackend for NoopStorageBackend {
    fn flush_batch(&mut self, _: &str, _: &[&str], _: &[String]) -> Result<(), String> {
        Ok(())
    }

    fn fetch_historic(&self, _: &str, _: i64) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}
