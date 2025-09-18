
with

tennisabstract_matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
),

-- union matches
matches_union as (
    select distinct
        *
    from (
        (
            select
                bk_match,
                match_date,
                match_year,
                match_gender,
                match_tournament,
                match_round,
                match_players,
            from tennisabstract_matches
        )
    ) as m
),

-- create match title from id columns (used as default title value during joins)
matches_title as (
    select
        *,
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
        ) as match_title
    from matches_union
),

-- join data to matches
matches_joined as (
    select
        m.bk_match,
        m.match_date,
        -- m.match_year,
        m.match_gender,
        m.match_tournament,
        m_ta.bk_match_tournament,
        m.match_round,
        m.match_players,
        coalesce(
            m_ta.match_title,
            m.match_title
        ) as match_title,
    from matches_title as m
    left join tennisabstract_matches as m_ta on m.bk_match = m_ta.bk_match
)

select * from matches_joined