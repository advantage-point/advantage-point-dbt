{% macro convert_rank_to_integer(column_name) %}
    case
        -- remove quotes, check if only digits remain
        when regexp_contains(replace(replace(trim({{ column_name }}), '"', ''), "'", ''), r'^\d+$')
        then cast(replace(replace(trim({{ column_name }}), '"', ''), "'", '') as int)
        else null
    end
{% endmacro %}