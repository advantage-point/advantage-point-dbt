with

tennisabstract_sets as (
    select * from {{ ref('int__web__tennisabstract__sets') }}
),

-- calculate player scores
tennisabstract_set_scores as (
    select
        *,

        -- player one scores
        -- lpad(
        --     (
        --         case
        --             when bk_point_server = bk_point_player_one then set_score_in_match_server
        --             when bk_point_receiver = bk_point_player_one then set_score_in_match_receiver
        --             else null
        --         end
        --     ),
        --     2,
        --     '0'
        -- )  as set_score_in_match_player_one,
        case
            when bk_point_server = bk_point_player_one then set_score_in_match_server_int
            when bk_point_receiver = bk_point_player_one then set_score_in_match_receiver_int
            else null
        end as set_score_in_match_player_one,

        -- player 2 scores
        -- lpad(
        --     (
        --         case
        --             when bk_point_server = bk_point_player_two then set_score_in_match_server
        --             when bk_point_receiver = bk_point_player_two then set_score_in_match_receiver
        --             else null
        --         end
        --     ),
        --     2,
        --     '0'
        -- )  as set_score_in_match_player_two,
        case
            when bk_point_server = bk_point_player_two then set_score_in_match_server_int
            when bk_point_receiver = bk_point_player_two then set_score_in_match_receiver_int
            else null
        end as set_score_in_match_player_two,

    from tennisabstract_sets
),

-- union tennisabstract players
tennisabstract_set_player as (
    select
        *
    from (
        (
            select
                bk_set,
                bk_point_player_one as bk_player,
                set_score_in_match_player_one as player_score,
            from tennisabstract_set_scores
        )
        union all
        (
            select
                bk_set,
                bk_point_player_two as bk_player,
                set_score_in_match_player_two as player_score,
            from tennisabstract_set_scores
        )
    ) as s
),

-- union
set_player_union as (
    select distinct
        *
    from (
        (
            select
                bk_set,
                bk_player,
            from tennisabstract_set_player
        )
    ) as sp
        
),

final as (
    select
        sp.bk_set,
        sp.bk_player,

        sp_ta.player_score,
        
    from set_player_union as sp
    left join tennisabstract_set_player as sp_ta on 1=1
        and sp.bk_set = sp_ta.bk_set
        and sp.bk_player = sp_ta.bk_player
)

select * from final