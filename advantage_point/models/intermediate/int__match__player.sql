with

tennisabstract_matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
    where match_result is not null
),

-- union tennisabstract winner and loser
tennisabstract_match_player as (
    select
        *
    from (
        (
            select
                bk_match,
                bk_match_winner as bk_player,
                true as player_is_winner,
                match_score as player_score,
            from tennisabstract_matches
        )
        union all
        (
            select
                bk_match,
                bk_match_loser as bk_player,
                false as player_is_winner,
                {{ flip_score(
                    score_col='match_score'
                ) }} as player_score,
            from tennisabstract_matches
        )
    ) as m
),

-- union
match_player_union as (
    select distinct
        *
    from (
        (
            select
                bk_match,
                bk_player,
            from tennisabstract_match_player
        )
    ) as mp
        
),

final as (
    select
        mp.bk_match,
        mp.bk_player,
        mp_ta.player_is_winner,
        mp_ta.player_score,
    from match_player_union as mp
    left join tennisabstract_match_player as mp_ta on 1=1
        and mp.bk_match = mp_ta.bk_match
        and mp.bk_player = mp_ta.bk_player
)

select * from final


