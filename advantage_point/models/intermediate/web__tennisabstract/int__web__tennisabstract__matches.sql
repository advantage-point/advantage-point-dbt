
with

tennisabstract_matches as (
    select * from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- get year from date
tennisabstract_matches_match_year as (
    select
        *,
        extract(year from match_date) as match_year,
    
    from tennisabstract_matches
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

    from tennisabstract_matches_match_year
),

-- parse match winner
tennisabstract_matches_match_winner as (
    select
        *,
        -- match_result: {match_winner} d. {match_loser} {match_score}
        split(match_result, ' d.')[0] as match_winner
    from tennisabstract_matches_match_result
    where match_url not in (
        -- there are 2 urls for this match
        -- this one has an incorrect date that I fixed in the staging model
        -- I will still exclude it
        'https://www.tennisabstract.com/charting/1990409-W-Amelia_Island-F-Steffi_Graf-Arantxa_Sanchez_Vicario.html'
    )
),

-- parse match loser
tennisabstract_matches_match_loser as (
    select
        *,
        case
            when match_winner = match_player_one then match_player_two
            when match_winner = match_player_two then match_player_one
            else null
        end as match_loser
    from tennisabstract_matches_match_winner
),

-- parse match score
tennisabstract_matches_match_score as (
    select
        *,
        -- match_result: {match_winner} d. {match_loser} {match_score}
        split(match_result, match_loser || ' ')[1] as match_score
    from tennisabstract_matches_match_loser
),

-- create bks
tennisabstract_matches_bk as (
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
        {{ generate_bk_player(
            player_name_col='match_player_one',
            player_gender_col='match_gender'
        ) }} as bk_match_player_one,
        {{ generate_bk_player(
            player_name_col='match_player_two',
            player_gender_col='match_gender'
        ) }} as bk_match_player_two,
    from tennisabstract_matches_match_score
),

-- create player array string (for use in unique id)
tennisabstract_matches_match_players as (
    select
        *,
        (
            select array_agg(player order by player)
            from unnest(array[match_player_one, match_player_two]) as player
        ) as match_players,
        (
            select array_agg(player order by player)
            from unnest(array[bk_match_player_one, bk_match_player_two]) as player
        ) as bk_match_players
    from tennisabstract_matches_bk
),

-- override match title
tennisabstract_matches_match_title as (
    select
        * replace (
            (
                case
                    when contains_substr(match_title, '404 Not Found') then
                        concat(
                            cast(match_year as string),
                            ' ',
                            match_tournament,
                            ' ',
                            match_round,
                            ': ',
                            match_players[0],
                            ' vs ',
                            match_players[1]
                        )
                    else match_title
                end
            ) as match_title
        )

    from tennisabstract_matches_match_players
),

final as (
    select
        {{ generate_bk_match(
            bk_match_date_col='bk_match_date',
            bk_match_tournament_col='bk_match_tournament',
            match_round_col='match_round',
            bk_match_players_col='bk_match_players'
        ) }} as bk_match,
        match_url,
        bk_match_date,
        match_date,
        match_year,
        match_gender,
        match_tournament,
        bk_match_tournament,
        match_round,
        match_players,
        bk_match_players,
        match_player_one,
        match_player_two,
        bk_match_player_one,
        bk_match_player_two,
        match_title,
        match_result,
        match_pointlog,
        match_winner,
        match_loser,
        {{ generate_bk_player(
            player_name_col='match_winner',
            player_gender_col='match_gender'
        ) }} as bk_match_winner,
        {{ generate_bk_player(
            player_name_col='match_loser',
            player_gender_col='match_gender'
        ) }} as bk_match_loser,
        match_score,
    from tennisabstract_matches_match_title
)

select * from final