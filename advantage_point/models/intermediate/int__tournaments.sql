with

tennisabstract_tournaments as (
    select * from {{ ref('int__web__tennisabstract__tournaments') }}
),

tennisabstract_matches_tournaments as (
    select * from {{ ref('int__web__tennisabstract__matches__tournaments') }}
),

tennisabstract_tournament_formats as (
    select * from {{ ref('int__seed__tennisabstract_tournament_formats') }}
),

-- get 'default' rows from tournament formats
tennisabstract_tournament_formats_default as (
    select * from tennisabstract_tournament_formats
    where lower(tournament_name) = '_default'
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

        coalesce(
            tf.best_of_sets,
            tf_default.best_of_sets
        ) as best_of_sets,

        coalesce(
            tf.sets_to_win,
            tf_default.sets_to_win
        ) as sets_to_win,

        coalesce(
            tf.games_per_set,
            tf_default.games_per_set
        ) as games_per_set,

        coalesce(
            tf.tiebreak_trigger_game,
            tf_default.tiebreak_trigger_game
        ) as tiebreak_trigger_game,

        coalesce(
            tf.tiebreak_points,
            tf_default.tiebreak_points
        ) as tiebreak_points,

        coalesce(
            tf.final_set_tiebreak_trigger_game,
            tf_default.final_set_tiebreak_trigger_game
        ) as final_set_tiebreak_trigger_game,

        coalesce(
            tf.final_set_tiebreak_points,
            tf_default.final_set_tiebreak_points
        ) as final_set_tiebreak_points,

        coalesce(
            tf.is_ad_scoring,
            tf_default.is_ad_scoring
        ) as is_ad_scoring,

    from tournaments_tour as t
    left join tennisabstract_tournaments as t_ta on t.bk_tournament = t_ta.bk_tournament
    left join tennisabstract_matches_tournaments as m_ta on t.bk_tournament = m_ta.bk_tournament
    left join tennisabstract_tournament_formats as tf on t.bk_tournament = tf.bk_tournament
    left join tennisabstract_tournament_formats_default as tf_default on lower(t.tournament_gender) = lower(tf_default.tournament_gender)
)

select * from final