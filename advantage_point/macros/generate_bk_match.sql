{% macro generate_bk_match(
    match_date_col,
    match_gender_col,
    match_tournament_col,
    match_round_col,
    match_players_col
) %}

    concat(
        cast({{ match_date_col }} as string),
        '||',
        lower({{ match_gender_col }}),
        '||',
        lower({{ match_tournament_col }}),
        '||',
        lower({{ match_round_col }}),
        '||',
        lower(array_to_string({{ match_players_col }}, ', '))
    )
    
{% endmacro %}
