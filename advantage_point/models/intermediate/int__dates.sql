with

utils_date_spine as (
    select * from {{ ref('int__utils__date_spine') }}
),

-- union dates
dates_union as (
    select distinct
        *
    from (
        (
            select
                bk_date,
                date_day,
            from utils_date_spine
        )
    ) as d
),

final as (
    select
        d.bk_date,
        d.date_day,

        d_uds.date_year,
        d_uds.date_month,
        d_uds.date_month_name,
        d_uds.day_of_month,
        d_uds.day_of_week,
        d_uds.day_name,
        d_uds.is_weekend,

    from dates_union as d
    left join utils_date_spine as d_uds on d.bk_date = d_uds.bk_date
)

select * from final