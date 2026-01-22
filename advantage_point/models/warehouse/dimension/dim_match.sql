-- NOTE:
-- This model represents TennisAbstract-only match data.
-- If additional match sources are added in the future,
-- introduce an int_match_unified model to reconcile them.

with

tennisabstract_matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
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

        {{ generate_sk_date(
            bk_date_col='bk_match_date'
        ) }} as sk_match_date,

    from tennisabstract_matches
),

final as (
    select
        sk_match,
        bk_match,
        sk_match_tournament,
        bk_match_tournament,
        sk_match_date,
        bk_match_date,
        bk_match_players_array,
        match_round,
        match_title,
    
    from match_sks
)

select * from final