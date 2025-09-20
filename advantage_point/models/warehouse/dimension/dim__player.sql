with

int_player as (
    select * from {{ ref('int__players') }}
),

dim_date as (
    select * from {{ ref('dim__date') }}
),

-- generate sk
player_sks as (
    select
        *,
        {{ generate_sk_player(
            bk_player_col='bk_player'
        ) }} as sk_player,
    from int_player
),

final as (
    select
        p.sk_player,
        p.bk_player,

        -- core attributes
        p.player_name,
        p.player_full_name,
        p.player_last_name,
        p.player_gender,
        d_birth_date.sk_date as sk_player_birth_date,
        p.bk_player_birth_date,
        p.player_country,
        p.player_height_in_cm,
        p.player_hand,
        p.player_backhand,

        -- rankings
        p.player_current_singles_ranking,
        p.player_peak_singles_ranking,
        p.player_first_peak_singles_ranking_date,
        p.player_last_peak_singles_ranking_date,
        p.player_current_doubles_ranking,
        p.player_peak_doubles_ranking,
        p.player_first_peak_doubles_ranking_date,

        -- identifiers
        p.player_itf_id,
        p.player_tour_id,
        p.player_tour_id_name,
        p.player_team_cup_id,
        p.player_team_cup_id_name,
        p.player_wikipedia_id,
        p.player_wikipedia_url,

        -- metadata
        p.player_tennisabstract_photograph,
        p.player_tennisabstract_photograph_credit,
        p.player_tennisabstract_photograph_link,
        p.player_tennisabstract_photograph_url,
        p.player_twitter_x_handle,
        p.player_twitter_x_url,
        p.player_hand_plays,
        p.player_backhand_plays,

        -- status
        p.is_player_active,
        p.player_last_match_played_date,

    from player_sks as p
    left join dim_date as d_birth_date on p.bk_player_birth_date = d_birth_date.bk_date 
)

select * from final