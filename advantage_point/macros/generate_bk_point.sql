{% macro generate_bk_point(
    bk_match_col,
    point_number_col
) %}

    concat(
        lower({{ bk_match_col }}),
        '||',
        lpad(cast({{ point_number_col }} as string), 4, '0')
    )
    
{% endmacro %}
