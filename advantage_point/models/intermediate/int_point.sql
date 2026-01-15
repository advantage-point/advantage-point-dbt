with

tennisabstract_points as (
    select * from {{ ref('int_tennisabstract__points_enriched') }}
),

matches as (
    select * from {{ ref('int_match') }}
),

tournaments as (
    select * from {{ ref('int_tournament') }}
),

-- union points
points_union as (
    select distinct
        *
    from (
        (
            select
                bk_point,
                bk_match,
                point_number_in_match
            from tennisabstract_points
        )
    ) as p
),

final as (
    select
        p.bk_point,
        p.bk_match,
        p.point_number_in_match,

        p_ta.bk_game,

        p_ta.point_result,
        p_ta.rally_length,

         -- point number in set
        row_number() over (
            partition by p_ta.bk_match, p_ta.set_number_in_match
            order by p_ta.game_number_in_set, p_ta.point_number_in_match
        ) as point_number_in_set,

        -- point number in game
        row_number() over (
            partition by p_ta.bk_match, p_ta.set_number_in_match, p_ta.game_number_in_set
            order by p_ta.point_number_in_match
        ) as point_number_in_game,

        -- get side of court
        case
            -- determine side based on non-tiebreaker scores
            when p_ta.point_score_in_game in (
                '0-0', '0-30',
                '15-15', '15-40',
                '30-0', '30-30'
            ) then 'deuce'
            when p_ta.point_score_in_game = '40-40' and t.is_ad_scoring then 'deuce'
            when p_ta.point_score_in_game in (
                '0-15', '0-40',
                '15-0', '15-30',
                '30-15', '30-40',
                'AD-40', '40-AD'
            ) then 'ad'
            -- determine side based on tiebreak scores
            when mod(p_ta.point_score_in_game_server_int + p_ta.point_score_in_game_receiver_int, 2) = 0 then 'deuce'
            when mod(p_ta.point_score_in_game_server_int + p_ta.point_score_in_game_receiver_int, 2) != 0 then 'ad'
            else null
        end as point_side,

    from points_union as p
    left join tennisabstract_points as p_ta on p.bk_point = p_ta.bk_point
    left join matches as m on p.bk_match = m.bk_match
    left join tournaments as t on m.bk_tournament = t.bk_tournament
)

select * from final