with

tennisabstract_tournaments as (
    select * from {{ ref('int__web__tennisabstract__tournaments') }}
),

tennisabstract_matches_tournaments as (
    select * from {{ ref('int__web__tennisabstract__matches__tournaments') }}
),

-- union tournaments
tournaments_union as (
    select distinct
        *
    from (
        (
            select
                bk_tournament,
                tournament_year,
                tournament_gender,
                tournament_name
            from tennisabstract_tournaments
        )
        union all
        (
            select
                bk_tournament,
                tournament_year,
                tournament_gender,
                tournament_name
            from tennisabstract_matches_tournaments
        )
    ) as t
),

-- create tournament title from id columns (used as default title value during joins)
tournaments_title as (
    select
        *,
        concat(
            cast(tournament_year as string),
            ' ',
            tournament_name
        ) as tournament_title
    from tournaments_union
),

-- create tournament tour name from id columns (used as default title value during joins)
tournaments_tour as (
    select
        *,
        case
            when tournament_gender = 'M' then 'ATP'
            when tournament_gender = 'W' then 'WTA'
            else null
        end as tournament_tour_name,
    from tournaments_title
),

final as (
    select
        t.bk_tournament,
        t.tournament_year,
        t.tournament_gender,
        t.tournament_name,
        t_ta.bk_tournament_start_date,
        t_ta.tournament_start_date,
        t_ta.tournament_surface,
        t_ta.tournament_draw_size,

        t_ta.best_of_sets,
        t_ta.sets_to_win,
        t_ta.games_per_set,
        t_ta.tiebreak_trigger_game,
        t_ta.tiebreak_points,
        t_ta.final_set_tiebreak_trigger_game,
        t_ta.final_set_tiebreak_points,
        t_ta.is_ad_scoring,

        coalesce(
            t_ta.tournament_title,
            m_ta.tournament_title,
            t.tournament_title
        ) as tournament_title,

        coalesce(
            t_ta.tournament_tour_name,
            m_ta.tournament_tour_name,
            t.tournament_tour_name
        ) as tournament_tour_name,

    from tournaments_tour as t
    left join tennisabstract_tournaments as t_ta on t.bk_tournament = t_ta.bk_tournament
    left join tennisabstract_matches_tournaments as m_ta on t.bk_tournament = m_ta.bk_tournament
)

select * from final