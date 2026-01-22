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
    
    from shots_sks
)

select * from final