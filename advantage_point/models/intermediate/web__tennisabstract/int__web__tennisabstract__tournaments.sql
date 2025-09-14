with

tennisabstract_tournaments as (
    select * from {{ ref('stg__web__tennisabstract__tournaments') }}
    where audit_column__active_flag = true
),

-- create bk_tournament
tennisabstract_tournaments_bk_tournament as (
    select
        *,
        {{ generate_bk_tournament(
            tournament_year_col='tournament_year',
            tournament_gender_col='tournament_gender',
            tournament_name_col='tournament_name'
        )}} as bk_tournament
    from tennisabstract_tournaments
),

final as (
    select
        bk_tournament,
        tournament_year,
        tournament_gender,
        tournament_name,
        tournament_url,
        tournament_title,
        tournament_start_date,
        tournament_surface,
        tournament_draw_size,

        -- designate tournament tour based on gender
        case
            when tournament_gender = 'M' then 'ATP'
            when tournament_gender = 'W' then 'WTA'
            else null
        end as tournament_tour_name,
    
    from tennisabstract_tournaments_bk_tournament
)

select * from final