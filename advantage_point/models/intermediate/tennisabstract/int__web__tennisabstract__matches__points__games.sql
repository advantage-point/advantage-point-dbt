with

tennisabstract_matches_points as (
    select * from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- get game columns
tennisabstract_matches_points_games as (
    select
        *
    from tennisabstract_matches_points
    where is_last_point_in_game = true
),

-- create tiebreaker flag
tennisabstract_matches_points_games_tiebreaker as (
    select
        *,

        case
            when game_score_in_set in ('6-6') then true
            when game_score_in_set not in ('6-6') then false
            else null
        end as is_tiebreaker,
    
    from tennisabstract_matches_points_games
),

final as (
    select
        bk_game,
        bk_match,
        game_number_in_match,

        match_url,

        bk_set,
        game_number_in_set,

        bk_point_server as bk_game_server,
        bk_point_receiver as bk_game_receiver,

        game_score_in_set,
        game_score_in_set_server,
        game_score_in_set_receiver,
        game_score_in_set_server_int,
        game_score_in_set_receiver_int,

        is_tiebreaker,

    from tennisabstract_matches_points_games_tiebreaker
)

select * from final