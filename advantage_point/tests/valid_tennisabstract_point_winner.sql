{{
    config(
        enabled=true
    )
}}

with

points as (
    select
      *
    from {{ ref('int_tennisabstract__points_enriched') }}
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

select
  match_url,
  point_number_in_match,
  set_score_in_match,
  game_score_in_set,
  point_score_in_game
from winner_array
where array_length(winner_array) != 1