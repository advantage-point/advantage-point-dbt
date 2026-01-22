with

source as (
    select * from {{ source('web__tennisabstract', 'players_classic') }}
),

renamed as (
    select
        player_name,
        player_gender,
        player_classic_url,
        {{ remove_empty_string_from_source('fullname') }} as player_full_name,
        {{ remove_empty_string_from_source('lastname') }} as player_last_name,
        {{ convert_rank_to_integer('currentrank') }} as player_current_singles_ranking,
        {{ convert_rank_to_integer('peakrank') }} as player_peak_singles_ranking,
        parse_date(
            '%Y%m%d',
            {{ remove_empty_string_from_source('peakfirst') }}
        ) as player_first_peak_singles_ranking_date,
        parse_date(
            '%Y%m%d',
            {{ remove_empty_string_from_source('peaklast') }}
        ) as player_last_peak_singles_ranking_date,
        parse_date(
            '%Y%m%d',
            {{ remove_empty_string_from_source('dob') }}
        ) as player_date_of_birth,
        cast({{ remove_empty_string_from_source('ht') }} as int) as player_height_in_cm,
        {{ remove_empty_string_from_source('hand') }} as player_hand,
        {{ remove_empty_string_from_source('backhand') }} as player_backhand,
        {{ remove_empty_string_from_source('country') }} as player_country,
        shortlist,
        careerjs,
        cast(cast(active as int) as boolean) as is_player_active,
        parse_date(
            '%Y%m%d',
            case lastdate
                when '0' then null
                else {{ remove_empty_string_from_source('lastdate') }}
            end
        ) as player_last_match_played_date,
        {{ remove_empty_string_from_source('twitter') }} as player_twitter_handle,
        {{ convert_rank_to_integer('current_dubs') }} as player_current_doubles_ranking,
        {{ convert_rank_to_integer('peak_dubs') }} as player_peak_doubles_ranking,
        parse_date(
            '%Y%m%d',
            {{ remove_empty_string_from_source('peakfirst_dubs') }}
        ) as player_first_peak_doubles_ranking_date,
        {{ convert_rank_to_integer('liverank') }} as liverank,
        chartagg,
        {{ remove_empty_string_from_source('photog') }} as photograph,
        {{ remove_empty_string_from_source('photog_credit') }} as photograph_credit,
        {{ remove_empty_string_from_source('photog_link') }} as photograph_link,
        {{ remove_empty_string_from_source('itf_id') }} as player_itf_id,
        {{ remove_empty_string_from_source('atp_id') }} as player_tour_id,
        {{ remove_empty_string_from_source('dc_id') }} as player_team_cup_id,
        {{ remove_empty_string_from_source('wiki_id') }} as player_wikipedia_id,
        {{ convert_rank_to_integer('elo_rating') }} as elo_rating,
        {{ convert_rank_to_integer('elo_rank') }} as elo_ranking,
        json_extract_array(matchmx) as player_matchlog,
        audit_column__active_flag,
        audit_column__start_datetime_utc,
        audit_column__end_datetime_utc
    from source
)

select * from renamed