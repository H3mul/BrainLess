{% if drop_table %}
DROP TABLE IF EXISTS "{{ table_name }}";
{% endif %}

CREATE TABLE IF NOT EXISTS "{{ table_name }}" (
{% for column, column_type in columns.items() %}
    "{{ column }}" {{ column_type }},
{% endfor %}    UNIQUE ("time")
);

SELECT create_hypertable(
    '{{ table_name }}',
    'time',
    if_not_exists => TRUE
);
