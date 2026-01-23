{{
    config(
        enabled=false
    )
}}

with

match_player as (
    select * 
    from {{ ref('int__match__player') }}
),

-- group by match
match_agg as (
    select
        bk_match,
        count(*) as num_rows,
        sum(cast(is_winner as int64)) as sum_rows,
    from match_player
    group by all
)

select *
from match_agg
where 1=0
or num_rows != 2 -- match does not have 2 players
or sum_rows != 1 -- match does not have winner and loser