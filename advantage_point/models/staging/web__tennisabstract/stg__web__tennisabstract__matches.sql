with

source as (
    select
        *
    from {{ source('web__tennisabstract', 'matches') }}
),

renamed as (
    select
        match_url,
        parse_date(
            '%Y%m%d',
            -- need to hardcode some ill date values 
            case
                when match_url = 'https://www.tennisabstract.com/charting/20170890-W-Toronto-R32-Ashleigh_Barty-Elena_Vesnina.html'
                    then '20170809'
                when match_url = 'https://www.tennisabstract.com/charting/1990409-W-Amelia_Island-F-Steffi_Graf-Arantxa_Sanchez_Vicario.html'
                    then '19900415'
                else match_date
            end
        ) as match_date,
        match_gender,
        case
            when match_gender = 'M' then "men's singles"
            when match_gender = 'W' then "women's singles"
            else null
        end as match_event,
        replace(match_tournament, '_', ' ') as match_tournament,
        match_round,
        replace(match_player_one, '_', ' ') as match_player_one,
        replace(match_player_two, '_', ' ') as match_player_two,
        match_title,
        match_result,
        json_extract_array(match_pointlog) as match_pointlog,
        audit_column__active_flag,
        audit_column__start_datetime_utc,
        audit_column__end_datetime_utc
    from source
)

select * from renamed