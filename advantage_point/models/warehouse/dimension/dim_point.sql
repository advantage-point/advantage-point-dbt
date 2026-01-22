-- NOTE:
-- This model represents TennisAbstract-only point data.
-- If additional point sources are added in the future,
-- introduce an int_point_unified model to reconcile them.

with

tennisabstract_points as (
    select * from {{ ref('int_tennisabstract__points_enriched') }}
),

-- create surrogate keys
points_sks as (
    select
        *,

        {{ generate_sk_point(
            bk_point_col='bk_point'
        )}} as sk_point,

        {{ generate_sk_match(
            bk_match_col='bk_match'
        )}} as sk_match,

        {{ generate_sk_set(
            bk_set_col='bk_set'
        )}} as sk_set,

        {{ generate_sk_game(
            bk_game_col='bk_game'
        )}} as sk_game,

    from tennisabstract_points
),

final as (

    select
        sk_point,
        bk_point,

        sk_match,
        bk_match,
        sk_set,
        bk_set,
        sk_game,
        bk_game,

        point_number_in_match,
        point_number_in_set,
        point_number_in_game,

        point_result,
        point_side,
        rally_length,

    from points_sks

)

select * from final
