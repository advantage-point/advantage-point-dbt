with

tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

-- create bks
tournament_bks as (
    select
        *,

        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_event_col='tournament_event',
            tournament_name_col='tournament_name'
        )}} as bk_tournament,

        {{ generate_bk_date(
            date_col='tournament_start_date'
        ) }} as bk_tournament_start_date,

    from tournaments
),

final as (
    select
        bk_tournament,
        tournament_year,
        tournament_event,
        tournament_name,
        tournament_url,
        tournament_title,
        bk_tournament_start_date,
        tournament_start_date,
        tournament_surface,
        tournament_draw_size,
    
    from tournament_bks
)

select * from final