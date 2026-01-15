with

int_tournament as (
    select * from {{ ref('int_tournament') }}
),

-- generate sk
tournament_sk as (
    select
        *,
        {{ generate_sk_tournament(
            bk_tournament_col='bk_tournament'
        ) }} as sk_tournament,
    from int_tournament
),

final as (
    select
        -- surrogate key
        t.sk_tournament,

        -- business key
        t.bk_tournament,

        -- core attributes
        t.tournament_year,
        t.tournament_event,
        t.tournament_name,
        t.tournament_title,

        -- optional attributes
        {{ generate_sk_date(
            bk_date_col='t.bk_tournament_start_date'
        ) }} as sk_tournament_start_date,
        t.bk_tournament_start_date,
        t.tournament_surface,
        t.tournament_draw_size,
        -- t.best_of_sets,
        -- t.sets_to_win,
        -- t.games_per_set,
        -- t.tiebreak_trigger_game,
        -- t.tiebreak_points,
        -- t.final_set_tiebreak_trigger_game,
        -- t.final_set_tiebreak_points,
        -- t.is_ad_scoring,

    from tournament_sk as t
)

select * from final