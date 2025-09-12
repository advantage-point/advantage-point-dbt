with

tennisabstract_tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

tennisabstract_matches as (
    select * from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- create tournament id
tennisabstract_tournaments_tournament_id as (
    select
        *,
        concat(
            cast(tournament_year as string),
            '||',
            tournament_gender,
            '||',
            tournament_name
        ) as tournament_id
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

-- create tournament id
tennisabstract_matches_tournaments_tournament_id as (
    select
        *,
        concat(
            cast(tournament_year as string),
            '||',
            tournament_gender,
            '||',
            tournament_name
        ) as tournament_id
    from tennisabstract_matches_tournaments
),

-- union tournaments
tournaments_union as (
    select distinct
        *
    from (
        (
            select
                tournament_id,
                tournament_year,
                tournament_gender,
                tournament_name
            from tennisabstract_tournaments_tournament_id
        )
        union all
        (
            select
                tournament_id,
                tournament_year,
                tournament_gender,
                tournament_name
            from tennisabstract_matches_tournaments_tournament_id
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
        t.tournament_id,
        t.tournament_year,
        t.tournament_gender,
        t.tournament_name,
        t_ta.tournament_start_date,
        t_ta.tournament_surface,
        t_ta.tournament_draw_size,
        coalesce(
            t_ta.tournament_title,
            t.tournament_title
        ) as tournament_title
    from tournaments_title as t
    left join tennisabstract_tournaments_tournament_id as t_ta on t.tournament_id = t_ta.tournament_id
    left join tennisabstract_matches_tournaments_tournament_id as m_ta on t.tournament_id = m_ta.tournament_id
)

select * from tournaments_joined