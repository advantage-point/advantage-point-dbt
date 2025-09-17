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
    
        s_ta.bk_match,
        s_ta.point_number_in_match,
        s_ta.match_url,

        s_ta.bk_game,
        s_ta.bk_set,
        
        s_ta.shot_number,
        s_ta.shot_text,
        s_ta.serve_sort,
        s_ta.shot_direction,
        s_ta.shot_result,
        s_ta.shot_type,

    from shots_union as s
    left join tennisabstract_matches_points_shots as s_ta on s.bk_shot = s_ta.bk_shot
)

select * from final