with

int_tournament as (
    select * from {{ ref('int__tournaments') }}
),

final as (
    select
        -- surrogate key
        {{ generate_sk_tournament(
            bk_tournament_col='bk_tournament'
        ) }} as sk_tournament,

        -- business key
        bk_tournament,

        -- core attributes
        tournament_year,
        tournament_gender,
        tournament_name,
        tournament_title,
        tournament_tour_name,

        -- optional attributes
        tournament_start_date,
        tournament_surface,
        tournament_draw_size,

    from int_tournament
)

select * from final