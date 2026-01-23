with

match_players as (
    select * from {{ ref('int_tennisabstract__match_player') }}
),

-- generate surrogate key
match_player_sks as (
    select
        *,

        {{ generate_sk_match_player(
                bk_match_col='bk_match',
                bk_player_col='bk_player'
        ) }} as sk_match_player,

        {{ generate_sk_match(
            bk_match_col='bk_match'
        ) }} as sk_match,

        {{ generate_sk_player(
            bk_player_col='bk_player'
        ) }} as sk_player,
    
    from match_players
),

final as (
    select

        -- surrogate key
        mp.sk_match_player,

        -- foreign keys
        mp.sk_match,
        mp.sk_player,

        -- business keys
        mp.bk_match,
        mp.bk_player,

        -- attributes
        mp.is_winner,
        mp.score,

    from match_player_sks as mp
)

select * from final
