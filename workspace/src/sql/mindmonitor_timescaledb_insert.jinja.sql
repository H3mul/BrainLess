COPY "{{ table_name }}" (
{% for column in columns %}    "{{ column }}"{% if not loop.last %},{% endif %}
{% endfor %}
) FROM STDIN
