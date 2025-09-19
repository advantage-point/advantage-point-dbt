{% macro generate_sk_match(
    bk_match_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_match_col]
    ) }}

{% endmacro %}
