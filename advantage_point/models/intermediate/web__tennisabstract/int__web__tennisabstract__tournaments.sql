with

tennisabstract_tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

tennisabstract_tournament_formats as (
    select * from {{ ref('int__seed__tennisabstract_tournament_formats')}}
)

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
        t.*
        tf.best_of_sets,
        tf.games_per_set,
        tf.tiebreak_trigger_game,
        tf.tiebreak_points,
        tf.final_set_tiebreak_trigger_game,
        tf.final_set_tiebreak_points,
        tf.is_ad_scoring,

    from tennisabstract_tournaments_bk_tournament as t
    left join tennisabstract_tournament_formats as tf on t.bk_tournament = tf.bk_tournament
)

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