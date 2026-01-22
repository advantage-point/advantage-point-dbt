{% macro generate_sk_point_player(
    bk_point_col,
    bk_player_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [
            bk_point_col,
            bk_player_col
        ]
    ) }}

{% endmacro %}
