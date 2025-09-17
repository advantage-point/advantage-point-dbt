with

tennisabstract_matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
),

-- get tournaments from match data
tennisabstract_matches_tournaments as (
    select distinct
        match_year as tournament_year,
        match_gender as tournament_gender,
        match_tournament as tournament_name
    from tennisabstract_matches
),

-- create bk_tournament
tennisabstract_matches_tournaments_bk_tournament as (
    select
        *,
        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        )}} as bk_tournament
    from tennisabstract_matches_tournaments
),

-- add a row number
-- in the example where there are 2 rows:
    -- Rio de Janeiro
    -- Rio De Janeiro
-- filtered out in next cte
tennisabstract_matches_tournament_row_num as (
    select
        *,

        -- order by tournament DESC so that the 'more capitalized' version captured
        row_number() over (partition by bk_tournament order by tournament_name desc) as bk_tournament_row_num
    from tennisabstract_matches_tournaments_bk_tournament
),

final as (
    select
        bk_tournament,
        tournament_year,
        tournament_gender,
        tournament_name,

        concat(
            cast(tournament_year as string),
            ' ',
            tournament_name
        ) as tournament_title,

        -- designate tournament tour based on gender
        case
            when tournament_gender = 'M' then 'ATP'
            when tournament_gender = 'W' then 'WTA'
            else null
        end as tournament_tour_name,
    from tennisabstract_matches_tournament_row_num
    where bk_tournament_row_num = 1
)

select * from final