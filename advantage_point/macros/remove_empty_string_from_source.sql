{% macro remove_empty_string_from_source(column_name) %}
    nullif(
        case
            when {{ column_name }} = '""' then null       -- two double quotes
            when {{ column_name }} = '' then null         -- empty string
            else replace(replace({{ column_name }}, '"', ''), "'", '')
        end,
        ''
    )
{% endmacro %}
