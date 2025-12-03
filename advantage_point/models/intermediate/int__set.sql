-- materialized as table
    -- referenced in multiple downstream models
{{
    config(
        materialized='table',
        cluster_by=['bk_match',],
    )
}}

with

tennisabstract_points as (
    select * from {{ ref('int__web__tennisabstract__points__enriched') }}
),

tennisabstract_sets_points as (
    select
        *,

        -- order set score so that it's always {bigger number}-{smaller number}
        case
            when set_score_in_match_server_int > set_score_in_match_receiver_int
            then set_score_in_match
            when set_score_in_match_receiver_int > set_score_in_match_server_int
            then {{ flip_score(
                    score_col='set_score_in_match'
                ) }}
            else null
        end as set_score_in_match_norm,

    from tennisabstract_points
    where 1=1
    and game_score_in_set = '0-0'
    and point_score_in_game = '0-0'
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
            from tennisabstract_points
        )
    ) as s
),

final as (
    select
        s.bk_set,
        s.bk_match,
        s.set_number_in_match,

        s_p_ta.set_score_in_match_norm as set_score_in_match

    from sets_union as s
    left join tennisabstract_sets_points as s_p_ta on s.bk_set = s_p_ta.bk_set
)

select * from final