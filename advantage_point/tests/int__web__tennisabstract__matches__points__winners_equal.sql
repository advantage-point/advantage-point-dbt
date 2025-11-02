{{
    config(
        enabled=false
    )
}}

with

points as (
    select * 
    from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- create array of distinct, non-null winner values
winner_array as (
  select
    p.*,
    (
      select array_agg(x)
      from (
        select distinct x
        from unnest([p.bk_point_winner_result, p.bk_point_winner_next_point]) as x
        where x is not null
      )
    ) as winner_array
  from points as p
)

select *
from winner_array as wa
where array_length(wa.winner_array) > 1