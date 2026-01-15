with

tennisabstract_tournaments as (
    select
        *,
        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_event_col='tournament_event',
            tournament_name_col='tournament_name'
        )}} as bk_tournament,
    from {{ ref('stg_tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

tennisabstract_matches as (
    select
        *,
        match_year as tournament_year,
        match_event as tournament_event,
        match_tournament as tournament_name,
    from {{ ref('int_tennisabstract__matches') }}
),

-- tennisabstract_tournament_formats as (
--     select * from  ref('int__seed__tennisabstract_tournament_formats') 
-- ),

-- -- get 'default' rows from tournament formats
-- tennisabstract_tournament_formats_default as (
--     select * from tennisabstract_tournament_formats
--     where lower(tournament_name) = '_default'
-- ),

-- get tournaments from match data
tennisabstract_match_tournaments as (
    select distinct
        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_event_col='tournament_event',
            tournament_name_col='tournament_name'
        )}} as bk_tournament,
        tournament_year,
        tournament_event,
        tournament_name,

        -- order by tournament DESC so that the 'more capitalized' version captured
        -- in the example where there are 2 rows:
            -- Rio de Janeiro
            -- Rio De Janeiro
        row_number() over (partition by tournament_year, tournament_event, lower(tournament_name) order by tournament_name desc) as tournament_row_num

    from tennisabstract_matches
),

-- filter for row number
tennisabstract_match_tournaments_row_num as (
    select
        *
    from tennisabstract_match_tournaments
    where tournament_row_num = 1
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
                tournament_event,
                tournament_name
            from tennisabstract_tournaments
        )
        union all
        (
            select
                bk_tournament,
                tournament_year,
                tournament_event,
                tournament_name
            from tennisabstract_match_tournaments_row_num
        )
    ) as t
),

final as (
    select
        t.bk_tournament,
        t.tournament_year,
        t.tournament_event,
        t.tournament_name,
        {{ generate_bk_date(
            date_col='t_ta.tournament_start_date'
        ) }} as bk_tournament_start_date,
        t_ta.tournament_start_date,
        t_ta.tournament_surface,
        t_ta.tournament_draw_size,

        coalesce(
            t_ta.tournament_title,
            concat(
                cast(t.tournament_year as string),
                ' ',
                t.tournament_name
            )
        ) as tournament_title,

        -- coalesce(
        --     tf.best_of_sets,
        --     tf_default.best_of_sets
        -- ) as best_of_sets,

        -- coalesce(
        --     tf.sets_to_win,
        --     tf_default.sets_to_win
        -- ) as sets_to_win,

        -- coalesce(
        --     tf.games_per_set,
        --     tf_default.games_per_set
        -- ) as games_per_set,

        -- coalesce(
        --     tf.tiebreak_trigger_game,
        --     tf_default.tiebreak_trigger_game
        -- ) as tiebreak_trigger_game,

        -- coalesce(
        --     tf.tiebreak_points,
        --     tf_default.tiebreak_points
        -- ) as tiebreak_points,

        -- coalesce(
        --     tf.final_set_tiebreak_trigger_game,
        --     tf_default.final_set_tiebreak_trigger_game
        -- ) as final_set_tiebreak_trigger_game,

        -- coalesce(
        --     tf.final_set_tiebreak_points,
        --     tf_default.final_set_tiebreak_points
        -- ) as final_set_tiebreak_points,

        -- coalesce(
        --     tf.is_ad_scoring,
        --     tf_default.is_ad_scoring
        -- ) as is_ad_scoring,

    from tournaments_union as t
    left join tennisabstract_tournaments as t_ta on t.bk_tournament = t_ta.bk_tournament
    left join tennisabstract_match_tournaments_row_num as m_ta on t.bk_tournament = m_ta.bk_tournament
    -- left join tennisabstract_tournament_formats as tf on t.bk_tournament = tf.bk_tournament
    -- left join tennisabstract_tournament_formats_default as tf_default on lower(t.tournament_event) = lower(tf_default.tournament_event)
)

select * from final