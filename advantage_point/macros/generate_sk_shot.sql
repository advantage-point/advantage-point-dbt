{% macro generate_sk_shot(
    bk_shot_col
) %}
    {{ dbt_utils.generate_surrogate_key(
        [bk_shot_col]
    ) }}

{% endmacro %}
