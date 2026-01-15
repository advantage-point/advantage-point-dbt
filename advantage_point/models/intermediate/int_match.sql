
with

tennisabstract_matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
),

-- union matches
matches_union as (
    select distinct
        *
    from (
        (
            select
                bk_match,
                bk_match_date,
                bk_match_tournament,
                match_round,
                bk_match_players_array,
            from tennisabstract_matches
        )
    ) as m
),

-- join data to matches
matches_joined as (
    select
        m.bk_match,
        m.bk_match_date,
        m.bk_match_tournament,
        m.match_round,
        m.bk_match_players_array, 
        
       m_ta.match_title,

    from matches_union as m
    left join tennisabstract_matches as m_ta on m.bk_match = m_ta.bk_match
)

select * from matches_joined