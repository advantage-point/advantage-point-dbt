{% macro generate_sk_set(
    bk_set_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_set_col]
    ) }}

{% endmacro %}
