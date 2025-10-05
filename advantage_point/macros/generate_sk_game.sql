{% macro generate_sk_game(
    bk_game_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_game_col]
    ) }}

{% endmacro %}
