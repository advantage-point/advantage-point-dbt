{% macro generate_sk_set_player(
    bk_set_col,
    bk_player_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            bk_set_col,
            bk_player_col
        ]
    ) }}

{% endmacro %}
