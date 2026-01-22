with

point_players as (
    select * from {{ ref('int_point_player') }}
),

-- generate surrogate key
point_player_sks as (
    select
        *,

        {{ generate_sk_point_player(
                bk_point_col='bk_point',
                bk_player_col='bk_player'
        ) }} as sk_point_player,

        {{ generate_sk_point(
            bk_point_col='bk_point'
        ) }} as sk_point,

        {{ generate_sk_player(
            bk_player_col='bk_player'
        ) }} as sk_player,
    
    from point_players
),

final as (
    select

        -- surrogate key
        sk_point_player,

        -- foreign keys
        sk_point,
        sk_player,

        -- business keys
        bk_point,
        bk_player,

        -- attributes
        player_is_winner,
        player_is_server,
        player_point_score_in_game,
        player_point_score_in_game_int,

    from point_player_sks as pp
)

select * from final
