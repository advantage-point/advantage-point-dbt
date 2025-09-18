with

tennisabstract_matches_points as (
    select * from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- get set columns
tennisabstract_matches_points_sets as (
    select
        *
    from tennisabstract_matches_points
    where 1=1
        and point_score_in_game = '0-0' -- filter for 'beginning of game' rows
        and game_score_in_set = '0-0' -- filter for 'beginning of set' rows
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