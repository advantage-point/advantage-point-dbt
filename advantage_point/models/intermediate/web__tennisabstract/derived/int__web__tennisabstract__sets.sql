{{
    config(
        materialized='ephemeral'
    )
}}

with

tennisabstract_points as (
    select * from {{ ref('int__web__tennisabstract__points__enriched') }}
    where 1=1
        and game_score_in_set = '0-0'
        and point_score_in_game = '0-0'
)

select * from tennisabstract_points