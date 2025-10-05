with

int_games as (
    select * from {{ ref('int__games') }}
),

dim_set as (
    select * from {{ ref('dim__set') }}
),

-- create sks
game_sks as (
    select
        *,
        {{ generate_sk_game(
            bk_game_col='bk_game'
        ) }} as sk_game,
    from int_games
),

final as (
    select
        g.sk_game,
        g.bk_game,
        s.sk_set as sk_game_set,
        g.bk_set as bk_game_set,
        g.game_number_in_match,
        g.game_number_in_set,
        g.is_tiebreaker,
    from game_sks as g
    left join dim_set as s on g.bk_set = s.bk_set
)

select * from final