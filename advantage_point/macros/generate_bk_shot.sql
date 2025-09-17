{% macro generate_bk_shot(
    bk_point_col,
    shot_number_col
) %}

    concat(
        lower({{ bk_point_col }}),
        '||',
        lpad(cast({{ shot_number_col }} as string), 4, '0')
    )
    
{% endmacro %}