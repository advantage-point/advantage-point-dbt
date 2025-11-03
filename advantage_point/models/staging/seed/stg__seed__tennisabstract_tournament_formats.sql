with

source as (
    select
        *
    from {{ ref('raw__seed__tennisabstract_tournament_formats') }}
),

renamed as (
    select
        tournament_name,
        case
            when tournament_event = 'MS' then "men's singles"
            when tournament_event = 'WS' then "women's singles"
            else null
        end as tournament_event,
        cast(effective_start_year as int) as effective_start_year,
        cast(effective_end_year as int) as effective_end_year,
        cast(best_of_sets as int) as best_of_sets,
        cast(sets_to_win as int) as sets_to_win,
        cast(games_per_set as int) as games_per_set,
        tiebreak_trigger_game,
        cast(tiebreak_points as int) as tiebreak_points,
        final_set_tiebreak_trigger_game,
        cast(final_set_tiebreak_points as int) as final_set_tiebreak_points,
        cast(is_ad_scoring as boolean) as is_ad_scoring,
        notes,

    from source
)

select * from renamed