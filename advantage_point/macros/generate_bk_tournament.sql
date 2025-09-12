{% macro generate_bk_tournament(
    tournament_year_col,
    tournament_gender_col,
    tournament_name_col
) %}

    concat(
        cast({{ tournament_year_col }} as string),
        '||',
        {{ tournament_gender_col }},
        '||',
        {{ tournament_name_col }}
    )
    
{% endmacro %}
