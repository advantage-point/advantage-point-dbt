with

tennisabstract_tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

tennisabstract_matches as (
    select * from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- create bk_tournament
tennisabstract_tournaments_bk_tournament as (
    select
        *,
        concat(
            cast(tournament_year as string),
            '||',
            tournament_gender,
            '||',
            tournament_name
        ) as bk_tournament
    from tennisabstract_tournaments
),

-- get tournaments from match data
tennisabstract_matches_tournaments as (
    select distinct
        extract(year from match_date) as tournament_year,
        match_gender as tournament_gender,
        match_tournament as tournament_name
    from tennisabstract_matches
),

-- create bk_tournament
tennisabstract_matches_tournaments_bk_tournament as (
    select
        *,
        concat(
            cast(tournament_year as string),
            '||',
            tournament_gender,
            '||',
            tournament_name
        ) as bk_tournament
    from tennisabstract_matches_tournaments
),

-- union tournaments
tournaments_union as (
    select distinct
        *
    from (
        (
            select
                bk_tournament,
                tournament_year,
                tournament_gender,
                tournament_name
            from tennisabstract_tournaments_bk_tournament
        )
        union all
        (
            select
                bk_tournament,
                tournament_year,
                tournament_gender,
                tournament_name
            from tennisabstract_matches_tournaments_bk_tournament
        )
    ) as t
),

-- create tournament title from id columns (used as default title value during joins)
tournaments_title as (
    select
        *,
        concat(
            cast(tournament_year as string),
            ' ',
            tournament_name
        ) as tournament_title
    from tournaments_union
),

-- join data to tournaments
tournaments_joined as (
    select
        t.bk_tournament,
        t.tournament_year,
        t.tournament_gender,
        t.tournament_name,
        t_ta.tournament_start_date,
        t_ta.tournament_surface,
        t_ta.tournament_draw_size,

        coalesce(
            t_ta.tournament_title,
            t.tournament_title
        ) as tournament_title,

        case
            when t.tournament_gender = 'M' then 'ATP'
            when t.tournament_gender = 'W' then 'WTA'
            else null
        end as tournament_tour_name
    from tournaments_title as t
    left join tennisabstract_tournaments_bk_tournament as t_ta on t.bk_tournament = t_ta.bk_tournament
    left join tennisabstract_matches_tournaments_bk_tournament as m_ta on t.bk_tournament = m_ta.bk_tournament
)

select * from tournaments_joined