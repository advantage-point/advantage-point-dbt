{{
    config(
        materialized = 'table'
    )
}}

with

int_set_player as (
    select * from {{ ref('int__set__player') }}
),

dim_set as (
    select * from {{ ref('dim__set') }}
),

dim_player as (
    select * from {{ ref('dim__player') }}
),

-- generate surrogate key
set_player_sk as (
    select
        *,

        {{
            generate_sk_set_player(
                bk_set_col='bk_set',
                bk_player_col='bk_player'
        ) }} as sk_set_player,
    
    from int_set_player
),

final as (
    select

        -- surrogate key
        sp.sk_set_player,

        -- foreign keys
        d_s.sk_set,
        d_p.sk_player,

        -- business keys
        sp.bk_set,
        sp.bk_player,

        -- attributes
        sp.player_score,

    from set_player_sk as sp
    left join dim_set as d_s on sp.bk_set = d_s.bk_set
    left join dim_player as d_p on sp.bk_player = d_p.bk_player
)

select * from final
