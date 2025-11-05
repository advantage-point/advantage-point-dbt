{% macro generate_bk_match(
    bk_match_date_col,
    bk_match_tournament_col,
    match_round_col,
    bk_match_players_array_col
) %}

    concat(
        cast({{ bk_match_date_col }} as string),
        '_',
        lower({{ bk_match_tournament_col }}),
        '_',
        lower({{ match_round_col }}),
        '_',
        lower(
            array_to_string(
                (
                    select array_agg(player order by player)
                    from unnest({{ bk_match_players_array_col }}) as player
                ),
                ','
            )
        )
    )
    
{% endmacro %}
