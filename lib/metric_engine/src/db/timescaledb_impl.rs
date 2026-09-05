//! TimescaleDB storage backend.
//!
//! Enabled with the `timescaledb` feature. Connections use the synchronous
//! `postgres` client without TLS; values are exchanged as text parameters
//! with per-column casts generated from the types the server reports, so no
//! engine-side schema knowledge is required beyond the table DDL.
//!
//! Tables must be registered through [`TimescaleDbStorageBackend::register_metric_table`]
//! before flushing: it runs the metric's DDL, converts the table into a
//! hypertable chunked on `timestamp_ms`, and introspects the column types.

use crate::SampleRate;
use crate::db::backend::StorageBackend;
use postgres::types::ToSql;
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::sync::Mutex;
use tracing::{debug, info};

/// Timestamp column every metric table is chunked on. The flush protocol
/// places it as the first value of every row.
pub const TIMESTAMP_COLUMN: &str = "time";

/// Name and server-reported data type of one metric table column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

pub struct TimescaleDbStorageBackend {
    connection: Mutex<Client>,
    /// Column definitions introspected at registration time, keyed by table
    /// name. Provides the types used to cast the text parameters and the
    /// deterministic column order used by the fetch queries.
    table_columns: Mutex<HashMap<String, Vec<ColumnDef>>>,
    database: String,
}

impl TimescaleDbStorageBackend {
    pub fn new(connection_string: impl Into<String>) -> Result<Self, String> {
        let connection_string = connection_string.into();
        let connection =
            Client::connect(&connection_string, NoTls).map_err(|error| error.to_string())?;
        info!(database = %connection_string, "connected to TimescaleDB backend");
        Ok(Self {
            connection: Mutex::new(connection),
            table_columns: Mutex::new(HashMap::new()),
            database: connection_string,
        })
    }

    /// Executes the metric's DDL, converts the table into a hypertable, and
    /// introspects the resulting column types for flush query generation.
    /// Must be called once per metric table before flushing. Table names are
    /// engine-declared statics (`PersistentMetric::table_name`), never user
    /// input, so interpolating them into the hypertable call is safe.
    pub fn register_metric_table(
        &mut self,
        create_table_sql: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let mut connection = self.lock_connection()?;
        connection
            .batch_execute(create_table_sql)
            .map_err(|error| error.to_string())?;
        connection
            .batch_execute(&format!(
                "SELECT create_hypertable('{table_name}', '{TIMESTAMP_COLUMN}', \
                 if_not_exists => TRUE, migrate_data => TRUE);"
            ))
            .map_err(|error| error.to_string())?;

        let rows = connection
            .query(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_name = $1 ORDER BY ordinal_position",
                &[&table_name],
            )
            .map_err(|error| error.to_string())?;
        let columns: Vec<ColumnDef> = rows
            .iter()
            .map(|row| ColumnDef {
                name: row.get(0),
                data_type: row.get(1),
            })
            .collect();
        if columns.is_empty() {
            return Err(format!(
                "table '{table_name}' has no introspectable columns"
            ));
        }

        debug!(
            database = %self.database,
            table = table_name,
            columns = ?columns.iter().map(|column| &column.name).collect::<Vec<_>>(),
            "registered TimescaleDB hypertable for metric table"
        );
        self.table_columns
            .lock()
            .map_err(|error| error.to_string())?
            .insert(table_name.to_string(), columns);
        Ok(())
    }

    fn lock_connection(&self) -> Result<std::sync::MutexGuard<'_, Client>, String> {
        self.connection.lock().map_err(|error| error.to_string())
    }

    fn table_columns(&self, table_name: &str) -> Result<Vec<ColumnDef>, String> {
        self.table_columns
            .lock()
            .map_err(|error| error.to_string())?
            .get(table_name)
            .cloned()
            .ok_or_else(|| format!("table '{table_name}' was not registered with the backend"))
    }
}

/// Builds the parameterized INSERT for one flush batch.
///
/// Every placeholder is double-cast (`$n::text::<type>`): the first cast
/// fixes the parameter type to text so string values are accepted, the
/// second converts to the column's server type inside the database. The
/// first placeholder targets the timestamp column, the rest follow
/// `schema_columns` in order.
fn build_insert_sql(table_name: &str, columns: &[ColumnDef]) -> String {
    let placeholders: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("${}::text::{}", index + 1, column.data_type))
        .collect();
    let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    format!(
        "INSERT INTO {table_name} ({}) VALUES ({})",
        names.join(", "),
        placeholders.join(", ")
    )
}

/// Builds a SELECT returning every column as text, in registration order, so
/// rows can be formatted back into the engine's string row contract.
fn build_select_sql(table_name: &str, columns: &[ColumnDef]) -> String {
    let selections: Vec<String> = columns
        .iter()
        .map(|column| format!("{}::text", column.name))
        .collect();
    format!("SELECT {} FROM {table_name}", selections.join(", "))
}

/// Formats a fetched row into the engine's comma-joined string row contract
/// (timestamp first, then schema columns).
fn format_row(row: &postgres::Row) -> String {
    (0..row.len())
        .map(|index| {
            let value: String = row.get(index);
            value
        })
        .collect::<Vec<_>>()
        .join(",")
}

impl StorageBackend for TimescaleDbStorageBackend {
    fn flush_batch(
        &mut self,
        table_name: &str,
        schema_columns: &[&str],
        row_values: &[Vec<String>],
    ) -> Result<(), String> {
        if row_values.is_empty() {
            return Ok(());
        }

        // Resolve types for `timestamp_ms` plus every schema column by name.
        let mut columns = vec![ColumnDef {
            name: TIMESTAMP_COLUMN.to_string(),
            data_type: "bigint".to_string(),
        }];
        let introspected = self.table_columns(table_name)?;
        for &name in schema_columns {
            let data_type = &introspected
                .iter()
                .find(|column| column.name == name)
                .ok_or_else(|| {
                    format!("column '{name}' is not present in TimescaleDB table '{table_name}'")
                })?
                .data_type;
            columns.push(ColumnDef {
                name: name.to_string(),
                data_type: data_type.clone(),
            });
        }

        let insert_sql = build_insert_sql(table_name, &columns);
        debug!(
            database = %self.database,
            table = table_name,
            row_count = row_values.len(),
            %insert_sql,
            "flushing metric rows to TimescaleDB backend"
        );

        let mut connection = self.lock_connection()?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        for row in row_values {
            if row.len() != columns.len() {
                return Err(format!(
                    "flush row has {} values but table '{}' expects {}",
                    row.len(),
                    table_name,
                    columns.len()
                ));
            }
            let params: Vec<&(dyn ToSql + Sync)> = row
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();
            transaction
                .execute(insert_sql.as_str(), &params)
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;

        info!(
            database = %self.database,
            table = table_name,
            row_count = row_values.len(),
            "flushed metric rows to TimescaleDB backend"
        );
        Ok(())
    }

    fn fetch_historic_range(
        &self,
        table_name: &str,
        start_ms: i64,
        end_ms: i64,
        _sample_rate: SampleRate,
    ) -> Result<Vec<String>, String> {
        let columns = self.table_columns(table_name)?;
        let sql = format!(
            "{} WHERE {TIMESTAMP_COLUMN} BETWEEN $1 AND $2 ORDER BY {TIMESTAMP_COLUMN}",
            build_select_sql(table_name, &columns)
        );
        debug!(
            database = %self.database,
            table = table_name,
            start_ms,
            end_ms,
            "fetching historic metric range from TimescaleDB backend"
        );
        let rows = self
            .lock_connection()?
            .query(&sql, &[&start_ms, &end_ms])
            .map_err(|error| error.to_string())?;
        Ok(rows.iter().map(format_row).collect())
    }

    fn fetch_historic(
        &self,
        table_name: &str,
        window_ms: i64,
        _sample_rate: SampleRate,
    ) -> Result<Vec<String>, String> {
        let columns = self.table_columns(table_name)?;
        // The backend owns the definition of "now": the window is anchored on
        // database time so all fetches are consistent with server-side
        // chunking.
        let sql = format!(
            "{} WHERE {TIMESTAMP_COLUMN} > \
             (extract(epoch from now()) * 1000)::bigint - $1 \
             ORDER BY {TIMESTAMP_COLUMN}",
            build_select_sql(table_name, &columns)
        );
        debug!(
            database = %self.database,
            table = table_name,
            window_ms,
            "fetching recent metric rows from TimescaleDB backend"
        );
        let rows = self
            .lock_connection()?
            .query(&sql, &[&window_ms])
            .map_err(|error| error.to_string())?;
        Ok(rows.iter().map(format_row).collect())
    }
}

#[cfg(test)]
mod tests;
