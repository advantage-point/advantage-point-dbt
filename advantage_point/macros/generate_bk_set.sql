{% macro generate_bk_set(
    bk_match_col,
    set_number_col
) %}

    concat(
        lower({{ bk_match_col }}),
        '||',
        lpad(cast({{ set_number_col }} as string), 2, '0')
    )
    
{% endmacro %}
