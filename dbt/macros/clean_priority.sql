{% macro clean_priority(column_name) %}
    substring({{ column_name }}, 3)
{% endmacro %}