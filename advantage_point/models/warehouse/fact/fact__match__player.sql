{{
    config(
        materialized = 'table'
    )
}}

with

int_match_player as (
    select * from {{ ref('int__match__player') }}
),

dim_match as (
    select * from {{ ref('dim__match') }}
),

dim_player as (
    select * from {{ ref('dim__player') }}
),

-- generate surrogate key
match_player_sk as (
    select
        *,

        {{
            generate_sk_match_player(
                bk_match_col='bk_match',
                bk_player_col='bk_player'
        ) }} as sk_match_player,
    
    from int_match_player
),

final as (
    select

        -- surrogate key
        mp.sk_match_player,

        -- foreign keys
        d_m.sk_match,
        d_p.sk_player,

        -- business keys
        mp.bk_match,
        mp.bk_player,

        -- attributes
        mp.player_is_winner,
        mp.player_score,

    from match_player_sk as mp
    left join dim_match as d_m on mp.bk_match = d_m.bk_match
    left join dim_player as d_p on mp.bk_player = d_p.bk_player
)

select * from final
