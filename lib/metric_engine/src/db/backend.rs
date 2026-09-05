/// Persistence boundary implemented by an application or database adapter.
///
/// The engine supplies table metadata and pre-formatted rows; the backend owns
/// connections, transactions, retries, and historic data access.
use crate::engine::core::SampleRate;

pub trait StorageBackend: Send + Sync {
    fn is_noop(&self) -> bool {
        false
    }
    /// Writes a batch of rows to a metric table.
    ///
    /// Each row is the timestamp in milliseconds followed by the parameter
    /// values for `schema_columns`, in order.
    fn flush_batch(
        &mut self,
        table_name: &str,
        schema_columns: &[&str],
        row_values: &[Vec<String>],
    ) -> Result<(), String>;

    /// Fetches historic rows for an explicit timestamp range.
    fn fetch_historic_range(
        &self,
        table_name: &str,
        start_ms: i64,
        end_ms: i64,
        sample_rate: SampleRate,
    ) -> Result<Vec<String>, String>;

    /// Fetches the most recent historic rows for a relative time window.
    ///
    /// The backend owns the definition of “now” so SQL implementations can
    /// use database time or another appropriate current-time expression.
    fn fetch_historic(
        &self,
        table_name: &str,
        window_ms: i64,
        sample_rate: SampleRate,
    ) -> Result<Vec<String>, String>;
}

#[derive(Default)]

pub struct NoopStorageBackend;

impl StorageBackend for NoopStorageBackend {
    fn is_noop(&self) -> bool {
        true
    }
    fn flush_batch(&mut self, _: &str, _: &[&str], _: &[Vec<String>]) -> Result<(), String> {
        Ok(())
    }

    fn fetch_historic_range(
        &self,
        _: &str,
        _: i64,
        _: i64,
        _: SampleRate,
    ) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    fn fetch_historic(&self, _: &str, _: i64, _: SampleRate) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}
