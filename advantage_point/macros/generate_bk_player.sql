{% macro generate_bk_player(
    player_name_col,
    player_gender_col
) %}

    concat(
        {{ player_name_col }},
        '||',
        {{ player_gender_col }}
    )
    
{% endmacro %}
