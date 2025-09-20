{% macro generate_bk_match(
    bk_match_date_col,
    match_gender_col,
    bk_match_tournament_col,
    match_round_col,
    bk_match_players_col
) %}

    concat(
        cast({{ bk_match_date_col }} as string),
        '_',
        lower({{ match_gender_col }}),
        '_',
        lower({{ bk_match_tournament_col }}),
        '_',
        lower({{ match_round_col }}),
        '_',
        lower(array_to_string({{ bk_match_players_col }}, ', '))
    )
    
{% endmacro %}
