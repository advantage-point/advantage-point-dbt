-- materialized as table since referenced in multiple downstream models
{{
    config(
        materialized = 'table',
    )
}}

with

tennisabstract_matches as (
    select
        *,

        extract(year from match_date) as match_year,

        {{ generate_bk_player(
            player_name_col='match_player_one',
            player_gender_col='match_gender'
        )}} as bk_match_player_one,

        {{ generate_bk_player(
            player_name_col='match_player_two',
            player_gender_col='match_gender'
        )}} as bk_match_player_two,

    from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- fix some match result values
tennisabstract_matches_match_result as (
    select
        * replace (
            (
                case
                    when match_url = 'https://www.tennisabstract.com/charting/20241119-M-Montemar_CH-R32-Francesco_Passaro-Nicolai_Budkov_Kjaer.html'
                    then 'Nicolai Budkov Kjaer d. Francesco Passaro 7-6(4) 4-6 6-0'
                    else match_result
                end
            ) as match_result
        )

    from tennisabstract_matches
    where match_url not in (
    -- there are 2 urls for this match
    -- this one has an incorrect date that I fixed in the staging model
    -- I will still exclude it
    'https://www.tennisabstract.com/charting/1990409-W-Amelia_Island-F-Steffi_Graf-Arantxa_Sanchez_Vicario.html'
    )
),

-- generate match bks
tennisabstract_matches_bks as (
    select
        *,

        {{ generate_bk_date(
            date_col='match_date'
        ) }} as bk_match_date,

        {{ generate_bk_tournament(
            tournament_year_col='match_year',
            tournament_event_col='match_event',
            tournament_name_col='match_tournament'
        )}} as bk_match_tournament,

        (
            select array_agg(player order by player)
            from unnest(array[bk_match_player_one, bk_match_player_two]) as player
        ) as bk_match_players_array,

    from tennisabstract_matches_match_result
),

-- create coalesced match title
-- in case some titles are '404' error value
tennisabstract_matches_title as (
    select
        *,

        concat(
            cast(match_year as string),
            ' ',
            match_tournament,
            ' ',
            match_round,
            ': ',
            match_player_one,
            ' vs ',
            match_player_two
        ) as match_title_coalesce,

    from tennisabstract_matches_bks
),

-- parse match winner
tennisabstract_matches_winner as (
    select
        *,

        -- match_result: {match_winner} d. {match_loser} {match_score}
        split(match_result, ' d.')[0] as match_winner,

    from tennisabstract_matches_title
),

-- parse match loser (and other player/bk_player columns)
tennisabstract_matches_loser as (
    select
        *,

        -- get bk match winner
        case match_winner
            when match_player_one then bk_match_player_one
            when match_player_two then bk_match_player_two
            else null
        end as bk_match_winner,

        -- get match loser
        case match_winner
            when match_player_one then match_player_two
            when match_player_two then match_player_one
            else null
        end as match_loser,

        -- get bk match loser
        case match_winner
            when match_player_one then bk_match_player_two
            when match_player_two then bk_match_player_one
            else null
        end as bk_match_loser,

    from tennisabstract_matches_winner
),

-- parse match score
tennisabstract_matches_score as (
    select
        *,

        -- match_result: {match_winner} d. {match_loser} {match_score}
        split(match_result, match_loser || ' ')[1] as match_score,
        
    from tennisabstract_matches_loser
),

final as (
    select
        {{ generate_bk_match(
            bk_match_date_col='bk_match_date',
            bk_match_tournament_col='bk_match_tournament',
            match_round_col='match_round',
            bk_match_players_array_col='bk_match_players_array'
        ) }} as bk_match,

        bk_match_date,
        match_date,
        bk_match_tournament,
        match_year,
        match_event,
        match_tournament,
        match_round,
        bk_match_players_array,
        bk_match_player_one,
        bk_match_player_two,
        match_player_one,
        match_player_two,
        match_gender,

        match_url,        

        case
            when match_title is null or match_title = '404 Not Found' then match_title_coalesce
            else match_title
        end as match_title,
        
        match_result,
        match_pointlog,

        match_winner,
        bk_match_winner,
        match_loser,
        bk_match_loser,
        match_score,
        
    from tennisabstract_matches_score
)

select * from final