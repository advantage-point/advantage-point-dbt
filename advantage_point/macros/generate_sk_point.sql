{% macro generate_sk_point(
    bk_point_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_point_col]
    ) }}

{% endmacro %}
