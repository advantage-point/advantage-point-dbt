with

int_player as (
    select * from {{ ref('int__players') }}
),

final as (
    select
        -- surrogate key
        {{ generate_sk_player(
            bk_player_col='bk_player'
        ) }} as sk_player,

        -- business key
        bk_player,

        -- core attributes
        player_name,
        player_full_name,
        player_last_name,
        player_gender,
        player_date_of_birth,
        player_country,
        player_height_in_cm,
        player_hand,
        player_backhand,

        -- rankings
        player_current_singles_ranking,
        player_peak_singles_ranking,
        player_first_peak_singles_ranking_date,
        player_last_peak_singles_ranking_date,
        player_current_doubles_ranking,
        player_peak_doubles_ranking,
        player_first_peak_doubles_ranking_date,

        -- identifiers
        player_itf_id,
        player_tour_id,
        player_tour_id_name,
        player_team_cup_id,
        player_team_cup_id_name,
        player_wikipedia_id,
        player_wikipedia_url,

        -- metadata
        player_tennisabstract_photograph,
        player_tennisabstract_photograph_credit,
        player_tennisabstract_photograph_link,
        player_tennisabstract_photograph_url,
        player_twitter_x_handle,
        player_twitter_x_url,
        player_hand_plays,
        player_backhand_plays,

        -- status
        is_player_active,
        player_last_match_played_date,

    from int_player
)

select * from final