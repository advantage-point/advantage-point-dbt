{% macro generate_sk_tournament(
    bk_tournament_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_tournament_col]
    ) }}

{% endmacro %}
