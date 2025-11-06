{% macro generate_sk_match_player(
    bk_match_col,
    bk_player_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            bk_match_col,
            bk_player_col
        ]
    ) }}

{% endmacro %}
