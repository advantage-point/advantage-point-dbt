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
        sk_match_player,

        -- foreign keys
        sk_match,
        sk_player,

        -- business keys
        bk_match,
        bk_player,

        -- attributes
        is_match_winner,
        match_score,

    from match_player_sks
)

select * from final
