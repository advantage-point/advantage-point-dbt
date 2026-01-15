with

int_player as (
    select * from {{ ref('int_player') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
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
        p.player_name,
        p.player_gender,

        -- player attributes
        p.player_full_name,
        p.player_last_name,
        p.player_country,
        p.player_height_in_cm,
        p.player_hand_plays,
        p.player_backhand_plays,
        date_player_date_of_birth.sk_date as sk_player_date_of_birth,
        p.bk_player_date_of_birth,

        -- tours and associations
        p.player_itf_id,
        p.player_tour_id,
        p.player_tour_id_name,
        p.player_team_cup_id,
        p.player_team_cup_id_name,
        
        -- media
        p.player_tennisabstract_photograph_url,
        p.player_wikipedia_url,
        p.player_twitter_url,

        -- rankings and status
        p.is_player_active,
        p.player_current_singles_ranking,
        p.player_peak_singles_ranking,
        date_player_first_peak_singles_ranking_date.sk_date as sk_player_first_peak_singles_ranking_date,
        p.bk_player_first_peak_singles_ranking_date,
        date_player_last_peak_singles_ranking_date.sk_date as sk_player_last_peak_singles_ranking_date,
        p.bk_player_last_peak_singles_ranking_date,
        p.player_current_doubles_ranking,
        p.player_peak_doubles_ranking,
        date_player_first_peak_doubles_ranking_date.sk_date as sk_player_first_peak_doubles_ranking_date,
        p.bk_player_first_peak_doubles_ranking_date, 
        date_player_last_match_played_date.sk_date as sk_player_last_match_played_date,
        p.bk_player_last_match_played_date,

    from player_sks as p
    left join dim_date as date_player_date_of_birth on p.bk_player_date_of_birth = date_player_date_of_birth.bk_date 
    left join dim_date as date_player_first_peak_singles_ranking_date on p.bk_player_first_peak_singles_ranking_date = date_player_first_peak_singles_ranking_date.bk_date 
    left join dim_date as date_player_last_peak_singles_ranking_date on p.bk_player_last_peak_singles_ranking_date = date_player_last_peak_singles_ranking_date.bk_date 
    left join dim_date as date_player_first_peak_doubles_ranking_date on p.bk_player_first_peak_doubles_ranking_date = date_player_first_peak_doubles_ranking_date.bk_date 
    left join dim_date as date_player_last_match_played_date on p.bk_player_last_match_played_date = date_player_last_match_played_date.bk_date 
    
)

select * from final