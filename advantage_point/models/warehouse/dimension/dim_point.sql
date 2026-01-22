with

points as (
    select * from {{ ref('int_point') }}
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

    from points
),

final as (

    select
        sk_point,
        bk_point,

        sk_match,
        bk_match,
        bk_set,
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
