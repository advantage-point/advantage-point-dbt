with

matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
),

-- get tournaments from match data
match_tournaments as (
    select distinct
        bk_match_tournament as bk_tournament,
        match_year as tournament_year,
        match_event as tournament_event,
        match_tournament as tournament_name,
    from matches
),

-- add a row number
-- in the example where there are 2 rows:
    -- Rio de Janeiro
    -- Rio De Janeiro
-- filtered out in next cte
match_tournaments_row_num as (
    select
        *,

        -- order by tournament DESC so that the 'more capitalized' version captured
        row_number() over (partition by bk_tournament order by tournament_name desc) as bk_tournament_row_num
    from match_tournaments
),

final as (
    select
        bk_tournament,
        tournament_year,
        tournament_event,
        tournament_name,

    from match_tournaments_row_num
    where bk_tournament_row_num = 1
)

select * from final