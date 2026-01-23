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
                bk_point_player_one = bk_point_winner as is_winner,
                bk_point_player_one = bk_point_server as is_server,
                point_score_in_game_player_one as score,
                point_score_in_game_player_one_int as score_int,
                
            from tennisabstract_points
        )
        union all
        (
            select
                bk_point,
                bk_point_player_two as bk_player,
                bk_point_player_two = bk_point_winner as is_winner,
                bk_point_player_two = bk_point_server as is_server,
                point_score_in_game_player_two as score,
                point_score_in_game_player_two_int as score_int,
                
            from tennisabstract_points
        )
    ) as p
),

-- union
point_player_union as (
    select distinct
        *
    from (
        (
            select
                bk_point,
                bk_player,
            from tennisabstract_point_player
        )
    ) as pp
        
),

final as (
    select
        pp.bk_point,
        pp.bk_player,
        pp_ta.is_winner,
        pp_ta.is_server,
        pp_ta.score,
        pp_ta.score_int,
    from point_player_union as pp
    left join tennisabstract_point_player as pp_ta on 1=1
        and pp.bk_point = pp_ta.bk_point
        and pp.bk_player = pp_ta.bk_player
)

select * from final