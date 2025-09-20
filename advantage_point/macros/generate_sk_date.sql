{% macro generate_sk_date(
    bk_date_col
) %}

    cast(format_date('%Y%m%d', {{ bk_date_col }}) as int64)

    
{% endmacro %}
