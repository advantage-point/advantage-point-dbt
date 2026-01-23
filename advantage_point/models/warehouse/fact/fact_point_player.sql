with

point_players as (
    select * from {{ ref('int_tennisabstract__point_player') }}
),

points as (
    select * from {{ ref('dim_point') }}
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

-- join models
point_player_joined as (
    select
        f_pp.*,

        -- determine game point
        -- TODO: figure out if ad scoring is used when '40-40'
        case
            when 1=1
                and f_pp.is_server = true
                and d_point.point_score_in_game in ('40-0', '40-15', '40-30', 'AD-40')
            then true
            else false
        end as is_game_point,

        -- determine game point
        -- TODO: figure out if ad scoring is used when '40-40'
        case
            when 1=1
                and f_pp.is_server = false
                and d_point.point_score_in_game in ('0-40', '15-40', '30-40', '40-AD')
            then true
            else false
        end as is_break_point,

    from point_player_sks as f_pp
    left join points as d_point on f_pp.sk_point = d_point.sk_point
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
        score,
        score_int,

        -- stats
        is_winner,
        is_server,
        is_game_point,
        is_break_point,

    from point_player_joined as pp
)

select * from final
