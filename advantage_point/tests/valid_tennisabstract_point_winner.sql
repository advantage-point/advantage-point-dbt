{{
    config(
        enabled=false
    )
}}

with

points as (
    select
      *
    from {{ ref('int_tennisabstract__points_enriched') }}
),

-- create array of distinct, non-null winner values
winners as (
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
  point_score_in_game,
  array_to_string(point_shotlog, ';\n') as point_shotlog_string,
  array_to_string(winner_array, ';\n') as winner_array_string,
  bk_point_winner,
from winners
where array_length(winner_array) != 1