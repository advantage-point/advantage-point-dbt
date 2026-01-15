{% test one_true_per_id(
    model,
    id_column,
    active_flag_column
) %}
    with
        id_agg as (
            select
                {{ id_column }} as id,
                count(*) as id_count
            from {{ model }}
            where {{ active_flag_column }} = True
            group by all
        )
    select
        *
    from id_agg
    where id_count > 1
{% endtest %}