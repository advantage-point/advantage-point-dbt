with

tennisabstract_tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

tennisabstract_tournament_formats as (
    select * from {{ ref('int__seed__tennisabstract_tournament_formats') }}
),

-- get 'default' rows from tournament formats
tennisabstract_tournament_formats_default as (
    select * from tennisabstract_tournament_formats
    where lower(tournament_name) = '_default'
),

-- create bk_tournament
tennisabstract_tournaments_bk_tournament as (
    select
        *,
        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        )}} as bk_tournament,
        {{ generate_bk_date(
            date_col='tournament_start_date'
        ) }} as bk_tournament_start_date,
    from tennisabstract_tournaments
),

-- join tournament formats
tennisabstract_tournaments_joined_formats as (
    select
        t.*,

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

    from tennisabstract_tournaments_bk_tournament as t
    left join tennisabstract_tournament_formats as tf on t.bk_tournament = tf.bk_tournament
    left join tennisabstract_tournament_formats_default as tf_default on lower(t.tournament_gender) = lower(tf_default.tournament_gender)
),

final as (
    select
        bk_tournament,
        tournament_year,
        tournament_gender,
        tournament_name,
        tournament_url,
        tournament_title,
        bk_tournament_start_date,
        tournament_start_date,
        tournament_surface,
        tournament_draw_size,

        best_of_sets,
        sets_to_win,
        games_per_set,
        tiebreak_trigger_game,
        tiebreak_points,
        final_set_tiebreak_trigger_game,
        final_set_tiebreak_points,
        is_ad_scoring,

        -- designate tournament tour based on gender
        case
            when tournament_gender = 'M' then 'ATP'
            when tournament_gender = 'W' then 'WTA'
            else null
        end as tournament_tour_name,
    
    from tennisabstract_tournaments_joined_formats
)

select * from final