{{
    config(
        materialized='table'
    )
}}

with

tennisabstract_matches_points as (
    select * from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- union points
points_union as (
    select distinct
        *
    from (
        (
            select
                bk_point,
                bk_match,
                point_number_in_match
            from tennisabstract_matches_points
        )
    ) as p
),

final as (
    select
        p.bk_point,
        p.bk_match,
        p.point_number_in_match,

        p_ta.bk_game,
        p_ta.bk_set,

        p_ta.point_server,
        p_ta.point_receiver,
        p_ta.bk_server,
        p_ta.bk_receiver,

        p_ta.set_score_in_match,
        p_ta.set_score_in_match_server,
        p_ta.set_score_in_match_receiver,

        p_ta.game_score_in_set,
        p_ta.game_score_in_set_server,
        p_ta.game_score_in_set_receiver,

        p_ta.point_score_in_game,
        p_ta.point_score_in_game_server,
        p_ta.point_score_in_game_receiver,

        p_ta.set_number_in_match,

        p_ta.game_number_in_match,
        p_ta.game_number_in_set,

        p_ta.point_number_in_set,
        p_ta.point_number_in_game,

        p_ta.point_side,
        p_ta.point_result,
        p_ta.number_of_shots,
        p_ta.rally_length,

        p_ta.point_winner,
        p_ta.point_loser,

        p_ta.is_break_point,
        p_ta.is_game_point,

    from points_union as p
    left join tennisabstract_matches_points as p_ta on p.bk_point = p_ta.bk_point
)

select * from final