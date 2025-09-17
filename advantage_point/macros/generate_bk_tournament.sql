{% macro generate_bk_tournament(
    tournament_year_col,
    tournament_gender_col,
    tournament_name_col
) %}

    concat(
        lower(cast({{ tournament_year_col }} as string)),
        '||',
        lower({{ tournament_gender_col }}),
        '||',
        lower({{ tournament_name_col }})
    )
    
{% endmacro %}
