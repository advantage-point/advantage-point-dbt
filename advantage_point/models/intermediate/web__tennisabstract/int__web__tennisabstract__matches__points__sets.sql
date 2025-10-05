with

tennisabstract_matches_points as (
    select * from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- get set columns
tennisabstract_matches_points_sets as (
    select
        *
    from tennisabstract_matches_points
    where is_last_point_in_set = true
),

final as (
    select
        bk_set,
        bk_match,
        set_number_in_match,

        match_url,

        set_score_in_match,
        set_score_in_match_server,
        set_score_in_match_receiver,
        set_score_in_match_server_int,
        set_score_in_match_receiver_int,

    from tennisabstract_matches_points_sets
)

select * from final