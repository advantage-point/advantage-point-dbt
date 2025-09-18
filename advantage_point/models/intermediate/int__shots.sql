with

tennisabstract_matches_points_shots as (
    select * from {{ ref('int__web__tennisabstract__matches__points__shots') }}
),

-- union shots
shots_union as (
    select distinct
        *
    from (
        (
            select
                bk_shot,
                bk_point,
                shot_number_in_point
            from tennisabstract_matches_points_shots
        )
    ) as s
),

final as (
    select
        s.bk_shot,
        s.bk_point,
        s.shot_number_in_point,
        
        s_ta.shot_number,
        s_ta.shot_direction,
        s_ta.shot_result,
        s_ta.shot_type,
        s_ta.bk_shot_player,

    from shots_union as s
    left join tennisabstract_matches_points_shots as s_ta on s.bk_shot = s_ta.bk_shot
)

select * from final