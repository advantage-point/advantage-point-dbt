-- NOTE:
-- This model represents TennisAbstract-only shot data.
-- If additional shot sources are added in the future,
-- introduce an int_shot_unified model to reconcile them.

with

tennisabstract_shots as (
    select * from {{ ref('int_tennisabstract__shots')}}
),

-- create surrogate keys
shots_sks as (
    select
        *,

        {{ generate_sk_shot(
            bk_shot_col='bk_shot'
        )}} as sk_shot,

        {{ generate_sk_point(
            bk_point_col='bk_point'
        )}} as sk_point,

        {{ generate_sk_game(
            bk_game_col='bk_game'
        )}} as sk_game,

        {{ generate_sk_set(
            bk_set_col='bk_set'
        )}} as sk_set,

        {{ generate_sk_match(
            bk_match_col='bk_match'
        )}} as sk_match,

        {{ generate_sk_player(
            bk_player_col='bk_shot_player'
        )}} as sk_shot_player,

    from tennisabstract_shots
),

-- calculate initial stats
shots_stats as (
    select
        *,

        shot_number = 1 as is_serve,
        shot_number = 1 and serve_sort = 1 as is_first_serve,
        shot_number = 1 and serve_sort = 2 as is_second_serve,
        shot_result = 'ace' as is_ace,
        shot_result = 'service winner' as is_service_winner,
        shot_result = 'double fault' as is_double_fault,
        shot_result in ('fault', 'double fault') as is_fault,
        shot_result = 'winner' as is_winner,
        shot_result = 'unforced error' as is_unforced_error,
        shot_result = 'forced error' as is_forced_error,
        shot_result in (
            'ace',
            'double fault',
            'winner',
            'unforced error',
            'forced error',
            'service winner'
        ) as is_point_ending_shot,

        shot_type like '%volley%' or shot_type like '%overhead%' as is_net_shot,

        shot_result in (
            'ace',
            'service winner',
            'winner'
        ) as is_hitter_winner,

        shot_result in (
            'double fault',
            'forced error',
            'unforced error'
        ) as is_hitter_error,

    from shots_sks
),

final as (
    select
        sk_shot,
        bk_shot,
        sk_point,
        bk_point,
        shot_number_in_point,

        sk_shot_player,
        bk_shot_player,

        sk_game,
        bk_game,
        sk_set,
        bk_set,
        sk_match,
        bk_match,
        
        shot_number,
        shot_direction,
        shot_result,
        shot_type,

        -- stats
        is_serve,
        is_first_serve,
        is_second_serve,
        is_ace,
        is_service_winner,
        is_double_fault,
        is_fault,
        is_winner,
        is_unforced_error,
        is_forced_error,
        is_point_ending_shot,
        is_net_shot,
        is_hitter_winner,
        is_hitter_error,
    
    from shots_stats
)

select * from final