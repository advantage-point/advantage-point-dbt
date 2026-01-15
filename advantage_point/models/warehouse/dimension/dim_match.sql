with

int_match as (
    select * from {{ ref('int_match') }}
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
        {{ generate_sk_tournament(
            bk_tournament_col='m.bk_match_tournament'
        ) }} as sk_match_tournament,
        m.bk_match_tournament,
        {{ generate_sk_date(
            bk_date_col='m.bk_match_date'
        ) }} as sk_match_date,
        m.bk_match_date,
        m.bk_match_players_array,
        m.match_round,
        m.match_title,
    
    from match_sks as m
)

select * from final