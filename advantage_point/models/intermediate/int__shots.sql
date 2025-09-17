with

tennisabstract_matches_points_shots as (
    select * from {{ ref('int__web__tennisabstract__matches__points__shots') }}
),

-- union shots
shots_union as (
    select distinct
        *
    from (
        (
            select
                bk_shot,
                bk_point,
                shot_number_in_point
            from tennisabstract_matches_points_shots
        )
    ) as s
),

final as (
    select
        s.bk_shot,
        s.bk_point,
        s.shot_number_in_point,
    
        s_ta.bk_match,
        s_ta.point_number_in_match,
        s_ta.match_url,

        s_ta.point_dict,
        s_ta.bk_game,
        s_ta.bk_set,
        s_ta.point_server,
        s_ta.point_receiver,
        s_ta.set_score_in_match,
        s_ta.game_score_in_set,
        s_ta.point_score_in_game,
        s_ta.point_shotlog,
        s_ta.set_score_in_match_server,
        s_ta.set_score_in_match_receiver,
        s_ta.game_score_in_set_server,
        s_ta.game_score_in_set_receiver,
        s_ta.point_score_in_game_server,
        s_ta.point_score_in_game_receiver,
        s_ta.set_score_in_match_server_int,
        s_ta.set_score_in_match_receiver_int,
        s_ta.game_score_in_set_server_int,
        s_ta.game_score_in_set_receiver_int,
        s_ta.point_score_in_game_server_int,
        s_ta.point_score_in_game_receiver_int,
        s_ta.set_number_in_match,
        s_ta.game_number_in_set,
        s_ta.game_number_in_match,
        s_ta.point_number_in_set,
        s_ta.point_number_in_game,
        s_ta.point_side,
        s_ta.point_result,
        s_ta.number_of_shots,
        s_ta.rally_length,
        s_ta.point_winner,
        s_ta.point_loser,
        s_ta.is_break_point,
        s_ta.is_game_point,
        s_ta.shot_number,
        s_ta.shot_text,
        s_ta.serve_sort,
        s_ta.shot_direction,
        s_ta.shot_result,
        s_ta.shot_type,

    from shots_union as s
    left join tennisabstract_matches_points_shots as s_ta on s.bk_shot = s_ta.bk_shot
)

select * from final