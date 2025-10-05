with

source as (
    select
        *
    from {{ ref('raw__seed__tennisabstract_matches_games') }}
),

renamed as (
    select
        match_url,
        game_server,
        set_score_in_match,
        game_score_in_set,
        game_winner,

    from source
)

select * from renamed