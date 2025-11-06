with

tennisabstract_matches_points_sets as (
    select * from {{ ref('int__web__tennisabstract__matches__points__sets') }}
),

-- union sets
sets_union as (
    select distinct
        *
    from (
        (
            select
                bk_set,
                bk_match,
                set_number_in_match
            from tennisabstract_matches_points_sets
        )
    ) as s
),

final as (
    select
        s.bk_set,
        s.bk_match,
        s.set_number_in_match,

    from sets_union as s
    left join tennisabstract_matches_points_sets as s_ta on s.bk_set = s_ta.bk_set
)

select * from final