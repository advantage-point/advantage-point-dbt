{% macro generate_bk_date(
    date_col
) %}

    safe_cast({{ date_col }} as date)
    
{% endmacro %}
