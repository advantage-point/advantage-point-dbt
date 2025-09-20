with

int_match as (
    select * from {{ ref('int__matches') }}
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
        {{ generate_sk_tournament(
            bk_tournament_col='bk_match_tournament'
        ) }} as sk_match_tournament,
    from int_match
),

final as (
    select
        -- surrogate keys
        m.sk_match,
        t.sk_tournament as sk_match_tournament,
        
        -- business key
        m.bk_match,
        m.bk_match_tournament,

        -- attributes
        m.match_date,
        m.match_gender,
        m.match_tournament,
        m.match_round,
        m.match_players,
        m.match_title,
    
    from match_sks as m
    left join dim_tournament as t on m.bk_match_tournament = t.bk_tournament
)

select * from final