pub trait StorageBackend: Send + Sync {
    fn flush_batch(
        &mut self,
        table_name: &str,
        schema_columns: &[&str],
        row_values: &[String],
    ) -> Result<(), String>;

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
