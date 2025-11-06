with

int_match as (
    select * from {{ ref('int__match') }}
),

dim_date as (
    select * from {{ ref('dim__date') }}
),

dim_tournament as (
    select * from {{ ref('dim__tournament') }}
),

-- create sks
match_sks as (
    select
        *,
        {{ generate_sk_match(
            bk_match_col='bk_match'
        ) }} as sk_match,
    from int_match
),

final as (
    select
        m.sk_match,
        m.bk_match,
        t.sk_tournament as sk_match_tournament,
        m.bk_match_tournament,
        d_match_date.sk_date as sk_match_date,
        m.bk_match_date,
        m.bk_match_players_array,
        m.match_round,
        m.match_title,
    
    from match_sks as m
    left join dim_tournament as t on m.bk_match_tournament = t.bk_tournament
    left join dim_date as d_match_date on m.bk_match_date = d_match_date.bk_date
)

select * from final