with

tennisabstract_tournament_formats as (
    select * from {{ ref('stg__seed__tennisabstract_tournament_formats') }}
),

tennisabstract_tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

-- get min/max tournament years
tennisabstract_tournaments_agg as (
    select
        tournament_name,
        tournament_event,
        min(tournament_year) as min_tournament_year,
        max(tournament_year) as max_tournament_year,
    
    from tennisabstract_tournaments
    group by all
),

-- join tournament agg years to format effective years (in case there are nulls)
tennisabstract_tournament_formats_effective_years as (
    select
        tf.* replace (

            -- effective_start_year
            (
                coalesce(
                    tf.effective_start_year, -- existing format effective start
                    t.min_tournament_year, -- earliest tournament year
                    extract(year from current_date()) -- current year
                )
            ) as effective_start_year,

            -- effective_end_year
            (
                coalesce(
                    tf.effective_end_year, -- existing format effective end
                    t.max_tournament_year, -- latest tournament year
                    extract(year from current_date()) -- current year
                )
            ) as effective_end_year
        )
    from tennisabstract_tournament_formats as tf
    left join tennisabstract_tournaments_agg as t on 1=1
        and lower(tf.tournament_name) = lower(t.tournament_name)
        and lower(tf.tournament_event) = lower(t.tournament_event)
),

-- explode into one row per effective year
tennisabstract_tournament_years as (
    select
        tf.*,
        year as tournament_year,
    from tennisabstract_tournament_formats_effective_years as tf,
    unnest(
        generate_array(
            tf.effective_start_year,
            tf.effective_end_year
        )
    ) as year
),

-- create bks
tennisabstract_tournament_bks as (
    select
        *,

        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_event_col='tournament_event',
            tournament_name_col='tournament_name'
        ) }} as bk_tournament,
    from tennisabstract_tournament_years
),

final as (
    select
        bk_tournament,
        tournament_year,
        tournament_event,
        tournament_name,

        best_of_sets,
        sets_to_win,
        games_per_set,
        tiebreak_trigger_game,
        tiebreak_points,
        final_set_tiebreak_trigger_game,
        final_set_tiebreak_points,
        is_ad_scoring,

    from tennisabstract_tournament_bks
)

select * from final