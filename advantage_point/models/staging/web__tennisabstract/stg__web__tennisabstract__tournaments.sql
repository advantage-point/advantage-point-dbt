with

source as (
    select
        *
    from {{ source('web__tennisabstract', 'tournaments') }}
),

renamed as (
    select
        tournament_url,
        cast(tournament_year as integer) as tournament_year,
        tournament_gender,
        case
            when tournament_gender = 'M' then "men's singles"
            when tournament_gender = 'W' then "women's singles"
            else null
        end as tournament_event,
        tournament_name,
        tournament_title,
        parse_date('%B %d, %Y', tournament_start_date) as tournament_start_date,
        tournament_surface,
        cast(tournament_draw_size as int) as tournament_draw_size,
        audit_column__active_flag,
        audit_column__start_datetime_utc,
        audit_column__end_datetime_utc
    from source
)

select * from renamed