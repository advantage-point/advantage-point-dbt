with

tennisabstract_matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
),

-- get players from match data
tennisabstract_matches_players as (
    select distinct
        *
    from (
        (
            select
                bk_match_player_one as bk_player,
                match_player_one as player_name,
                match_gender as player_gender
            from tennisabstract_matches
        )
        union all
        (
            select
                bk_match_player_two as bk_player,
                match_player_two as player_name,
                match_gender as player_gender
            from tennisabstract_matches
        )
    ) as p
),

final as (
    select
        bk_player,
        player_name,
        player_gender,
    from tennisabstract_matches_players
)

select * from final