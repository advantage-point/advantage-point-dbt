{% macro generate_bk_player(
    player_name_col,
    player_gender_col
) %}

    concat(
        lower({{ player_name_col }}),
        '||',
        lower({{ player_gender_col }})
    )
    
{% endmacro %}
