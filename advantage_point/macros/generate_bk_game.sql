{% macro generate_bk_game(
    bk_match_col,
    game_number_col
) %}

    concat(
        lower({{ bk_match_col }}),
        '||',
        lpad(cast({{ game_number_col }} as string), 4, '0')
    )
    
{% endmacro %}
