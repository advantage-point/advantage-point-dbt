with

tennisabstract_matches_points_games as (
    select * from {{ ref('int__web__tennisabstract__matches__points__games') }}
),

seed_tennisabstract_matches_games as (
    select * from {{ ref('int__seed__tennisabstract_matches_games') }}
),

-- union games data
tennisabstract_matches_games_union as (
    select distinct
        *
    from (
        (
            select
                bk_game,
                bk_match,
                game_number_in_match,

                match_url,

                bk_set,
                game_number_in_set,

                bk_game_server,
                bk_game_receiver,

                game_score_in_set,
                game_score_in_set_server,
                game_score_in_set_receiver,
                game_score_in_set_server_int,
                game_score_in_set_receiver_int,

                is_tiebreaker,
            from tennisabstract_matches_points_games
        )
        union all
        (
            select
                bk_game,
                bk_match,
                game_number_in_match,

                match_url,

                bk_set,
                game_number_in_set,

                bk_game_server,
                bk_game_receiver,

                game_score_in_set,
                game_score_in_set_server,
                game_score_in_set_receiver,
                game_score_in_set_server_int,
                game_score_in_set_receiver_int,

                is_tiebreaker,

            from seed_tennisabstract_matches_games
        )
    ) as g
)

select * from tennisabstract_matches_games_union