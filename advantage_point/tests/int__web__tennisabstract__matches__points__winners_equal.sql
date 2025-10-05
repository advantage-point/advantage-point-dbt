with

points as (
    select * from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- create array of winner columns
winner_array as (
    select
        *,

        array_distinct(
            [
                bk_point_winner_result,
                bk_point_winner_next_point
            ]
        ) as winner_array,
    
    from points
)

select *
from winner_array
where array_length(winner_array) > 1
