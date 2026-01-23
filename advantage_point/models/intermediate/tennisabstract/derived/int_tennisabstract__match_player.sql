with

tennisabstract_matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
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
                true as is_match_winner,
                match_score,
            from tennisabstract_matches
        )
        union all
        (
            select
                bk_match,
                bk_match_loser as bk_player,
                false as is_match_winner,
                {{ flip_score(
                    score_col='match_score'
                ) }} as match_score,
            from tennisabstract_matches
        )
    ) as m
),

final as (
    select
        bk_match,
        bk_player,
        is_match_winner,
        match_score,
    from tennisabstract_match_player
)

select * from final


