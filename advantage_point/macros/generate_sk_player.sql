{% macro generate_sk_player(
    bk_player_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_player_col]
    ) }}

{% endmacro %}
