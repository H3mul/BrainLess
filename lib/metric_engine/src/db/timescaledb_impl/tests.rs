use super::*;

fn column(name: &str, data_type: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: data_type.to_string(),
    }
}

#[test]
fn insert_sql_casts_every_parameter_from_text() {
    let columns = vec![
        column("timestamp_ms", "bigint"),
        column("value", "double precision"),
        column("label", "text"),
    ];

    assert_eq!(
        build_insert_sql("eeg", &columns),
        "INSERT INTO eeg (timestamp_ms, value, label) VALUES \
         ($1::text::bigint, $2::text::double precision, $3::text::text)"
    );
}

#[test]
fn select_sql_returns_columns_as_text_in_order() {
    let columns = vec![
        column("timestamp_ms", "bigint"),
        column("value", "double precision"),
    ];

    assert_eq!(
        build_select_sql("eeg", &columns),
        "SELECT timestamp_ms::text, value::text FROM eeg"
    );
}
