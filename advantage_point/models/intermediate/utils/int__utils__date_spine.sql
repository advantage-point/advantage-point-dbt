with

date_spine as (
    select * from {{ ref('stg__utils__date_spine') }}
),

-- create bk
date_bk as (
    select
        *,
        -- bk_date
        {{ generate_bk_date(
            date_col='date_day'
        ) }} as bk_date,
    from date_spine
),

final as (
    select
        bk_date,
        date_day,

        extract(year from date_day) as date_year,
        extract(month from date_day) as date_month,
        format_date('%B', date_day) as date_month_name,
        extract(day from date_day) as day_of_month,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,
        case
            when extract(dayofweek from date_day) in (1,7) then true
            when extract(dayofweek from date_day) not in (1,7) then false
            else null
        end as is_weekend,

    from date_bk
)

select * from final