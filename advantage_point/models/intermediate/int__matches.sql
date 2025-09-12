
with

tennisabstract_matches as (
    select * from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- parse match winner
tennisabstract_matches_match_winner as (
    select
        *,
        -- match_result: {match_winner} d. {match_loser} {match_score}
        split(match_result, ' d.')[0] as match_winner
    from tennisabstract_matches
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
-- create player array string (for use in unique id)
tennisabstract_matches_match_players as (
    select
        *,
        (
            select array_agg(player order by player)
            from unnest(array[match_player_one, match_player_two]) as player
        ) as match_players
    from tennisabstract_matches_match_score
),

-- create bk_match
tennisabstract_matches_bk_match as (
    select
        *,
        {{ generate_bk_match(
            match_date_col='match_date',
            match_gender_col='match_gender',
            match_tournament_col='match_tournament',
            match_round_col='match_round',
            match_players_col='match_players'
        ) }} as bk_match
    from tennisabstract_matches_match_players
),

-- union matches
matches_union as (
    (
        select
            bk_match,
            match_date,
            match_gender,
            match_tournament,
            match_round,
            match_players
        from tennisabstract_matches_bk_match
    )
),

-- create match title from id columns (used as default title value during joins)
matches_title as (
    select
        *,
        concat(
            cast(extract(year from match_date) as string),
            ' ',
            match_tournament,
            ' ',
            match_round,
            ': ',
            match_players[0],
            ' vs ',
            match_players[1]
        ) as match_title
    from matches_union
),

-- join data to matches
matches_joined as (
    select
        m.bk_match,
        m.match_date,
        m.match_gender,
        m.match_tournament,
        m.match_round,
        m.match_players,
        m_ta.match_winner,
        m_ta.match_loser,
        m_ta.match_score,
        coalesce(
            m_ta.match_title,
            m.match_title
        ) as match_title
    from matches_title as m
    left join tennisabstract_matches_bk_match as m_ta on m.bk_match = m_ta.bk_match
)

select * from matches_joined