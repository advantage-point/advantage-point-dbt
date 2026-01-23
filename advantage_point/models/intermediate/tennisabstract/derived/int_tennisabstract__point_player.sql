with

tennisabstract_points as (
    select * from {{ ref('int_tennisabstract__points_enriched') }}
),

-- union tennisabstract winner and loser
tennisabstract_point_player as (
    select
        *
    from (
        (
            select
                bk_point,
                bk_point_player_one as bk_player,
                bk_point_player_one = bk_point_winner as is_point_won,
                bk_point_player_one = bk_point_server as is_service_point,
                point_score_in_game_player_one as point_score_in_game,
                point_score_in_game_player_one_int as point_score_in_game_int,
                
            from tennisabstract_points
        )
        union all
        (
            select
                bk_point,
                bk_point_player_two as bk_player,
                bk_point_player_two = bk_point_winner as is_point_won,
                bk_point_player_two = bk_point_server as is_service_point,
                point_score_in_game_player_two as point_score_in_game,
                point_score_in_game_player_two_int as point_score_in_game_int,
                
            from tennisabstract_points
        )
    ) as p
),

final as (
    select
        bk_point,
        bk_player,
        is_point_won,
        is_service_point,
        not is_service_point as is_return_point,
        point_score_in_game,
        point_score_in_game_int,
    from tennisabstract_point_player
)

select * from final