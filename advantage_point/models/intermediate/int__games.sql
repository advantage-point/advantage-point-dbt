with

tennisabstract_matches_points_games as (
    select * from {{ ref('int__web__tennisabstract__matches__points__games') }}
),

-- union games
games_union as (
    select distinct
        *
    from (
        (
            select
                bk_game,
                bk_match,
                game_number_in_match
            from tennisabstract_matches_points_games
        )
    ) as g
),

final as (
    select
        g.bk_game,
        g.bk_match,
        g.game_number_in_match,

        g_ta.bk_set,
        g_ta.game_number_in_set,
        g_ta.is_tiebreaker,

    from games_union as g
    left join tennisabstract_matches_points_games as g_ta on g.bk_game = g_ta.bk_game
)

select * from final