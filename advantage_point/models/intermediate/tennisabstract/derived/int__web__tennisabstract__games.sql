-- materialized as table
    -- referenced in multiple downstream models
{{
    config(
        materialized='table',
        cluster_by=['bk_match', 'bk_set',],
    )
}}

with

tennisabstract_points as (
    select * from {{ ref('int_tennisabstract__points_enriched') }}
    where point_score_in_game = '0-0'
),

final as (
    select

        bk_game,
        bk_match,
        game_number_in_match,

        game_score_in_set,
        game_score_in_set_server,
        game_score_in_set_receiver,
        game_number_in_set,

        bk_set,

        bk_point_server as bk_game_server,
        bk_point_receiver as bk_game_receiver,

    from tennisabstract_points
)

select * from final