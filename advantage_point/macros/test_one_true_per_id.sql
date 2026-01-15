{% test one_true_per_id(
    model,
    id_column_array,
    active_flag_column
) %}

{% set composite_id %}
    {{ id_column_array | map('string') | join(" || '|' || ") }}
{% endset %}

with id_agg as (

    select
        {{ composite_id }} as composite_id,
        count(*) as id_count
    from {{ model }}
    where {{ active_flag_column }} = true
    group by composite_id

)

select *
from id_agg
where id_count > 1

{% endtest %}