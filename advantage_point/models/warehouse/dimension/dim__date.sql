with

int_date as (
    select * from {{ ref('int__dates') }}
),

-- create sk
date_sk as (
    select
        *,
        {{ generate_sk_date(
            bk_date_col='bk_date'
        ) }} as sk_date,
    from int_date
),

final as (
    select
        sk_date,
        bk_date,
        date_day,

        date_year,
        date_month,
        date_month_name,
        day_of_month,
        day_of_week,
        day_name,
        is_weekend,

    from date_sk
)

select * from final