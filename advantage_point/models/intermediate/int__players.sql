with

tennisabstract_players as (
    select * from {{ ref('int__web__tennisabstract__players') }}
),

tennisabstract_players_classic as (
    select * from {{ ref('int__web__tennisabstract__players_classic') }}
),

tennisabstract_matches_players as (
    select * from {{ ref('int__web__tennisabstract__matches__players') }}
),

-- union players
players_union as (
    select distinct
        *
    from (
        (
            select
                bk_player,
                player_name,
                player_gender
            from tennisabstract_players
        )
        union all
        (
            select
                bk_player,
                player_name,
                player_gender
            from tennisabstract_players_classic
        )
        union all
        (
            select
                bk_player,
                player_name,
                player_gender
            from tennisabstract_matches_players
        )
    ) as p
),

-- create default tour values
players_tour_values as (
    select
        *,
        -- since 'atp_id' was recoded in staging model to 'tour_id' (more generalizable), this denotes the tour name of the id
        case
            when player_gender = 'M' then 'ATP'
            when player_gender = 'W' then 'WTA'
            else null
        end as player_tour_id_name,

        -- since 'dc_id' was recoded in staging model to 'team_cup_id' (more generalizable), this denotes the team event name of the id
        case
            when player_gender = 'M' then 'Davis Cup'
            when player_gender = 'W' then 'Billie Jean King Cup'
            else null
        end as player_team_cup_id_name,
    from players_union
),

-- join data to players
final as (
    select
        p.bk_player,
        p.player_name,
        p.player_gender,

        coalesce(
            p_ta.player_full_name,
            pc_ta.player_full_name
        ) as player_full_name,

        coalesce(
            p_ta.player_last_name,
            pc_ta.player_last_name
        ) as player_last_name,

        coalesce(
            p_ta.player_current_singles_ranking,
            pc_ta.player_current_singles_ranking
        ) as player_current_singles_ranking,

        coalesce(
            p_ta.player_peak_singles_ranking,
            pc_ta.player_peak_singles_ranking
        ) as player_peak_singles_ranking,

        coalesce(
            p_ta.player_first_peak_singles_ranking_date,
            pc_ta.player_first_peak_singles_ranking_date
        ) as player_first_peak_singles_ranking_date,

        coalesce(
            p_ta.player_last_peak_singles_ranking_date,
            pc_ta.player_last_peak_singles_ranking_date
        ) as player_last_peak_singles_ranking_date,

        coalesce(
            p_ta.bk_player_birth_date,
            pc_ta.bk_player_birth_date
        ) as bk_player_birth_date,

        coalesce(
            p_ta.player_date_of_birth,
            pc_ta.player_date_of_birth
        ) as player_date_of_birth,

        coalesce(
            p_ta.player_height_in_cm,
            pc_ta.player_height_in_cm
        ) as player_height_in_cm,

        coalesce(
            p_ta.player_hand,
            pc_ta.player_hand
        ) as player_hand,

        coalesce(
            p_ta.player_backhand,
            pc_ta.player_backhand
        ) as player_backhand,

        coalesce(
            p_ta.player_country,
            pc_ta.player_country
        ) as player_country,

        coalesce(
            p_ta.is_player_active,
            pc_ta.is_player_active
        ) as is_player_active,

        coalesce(
            p_ta.player_last_match_played_date,
            pc_ta.player_last_match_played_date
        ) as player_last_match_played_date,

        coalesce(
            p_ta.player_twitter_x_handle,
            pc_ta.player_twitter_x_handle
        ) as player_twitter_x_handle,

        coalesce(
            p_ta.player_current_doubles_ranking,
            pc_ta.player_current_doubles_ranking
        ) as player_current_doubles_ranking,

        coalesce(
            p_ta.player_peak_doubles_ranking,
            pc_ta.player_peak_doubles_ranking
        ) as player_peak_doubles_ranking,

        coalesce(
            p_ta.player_first_peak_doubles_ranking_date,
            pc_ta.player_first_peak_doubles_ranking_date
        ) as player_first_peak_doubles_ranking_date,

        coalesce(
            p_ta.player_tennisabstract_photograph,
            pc_ta.player_tennisabstract_photograph
        ) as player_tennisabstract_photograph,

        coalesce(
            p_ta.player_tennisabstract_photograph_credit,
            pc_ta.player_tennisabstract_photograph_credit
        ) as player_tennisabstract_photograph_credit,

        coalesce(
            p_ta.player_tennisabstract_photograph_link,
            pc_ta.player_tennisabstract_photograph_link
        ) as player_tennisabstract_photograph_link,

        coalesce(
            p_ta.player_itf_id,
            pc_ta.player_itf_id
        ) as player_itf_id,

        coalesce(
            p_ta.player_tour_id,
            pc_ta.player_tour_id
        ) as player_tour_id,
        
        coalesce(
            p_ta.player_team_cup_id,
            pc_ta.player_team_cup_id
        ) as player_team_cup_id,

        coalesce(
            p_ta.player_wikipedia_id,
            pc_ta.player_wikipedia_id
        ) as player_wikipedia_id,

        coalesce(
            p_ta.player_backhand_plays,
            pc_ta.player_backhand_plays
        ) as player_backhand_plays,

        coalesce(
            p_ta.player_tennisabstract_photograph_url,
            pc_ta.player_tennisabstract_photograph_url
        ) as player_tennisabstract_photograph_url,

        coalesce(
            p_ta.player_twitter_x_url,
            pc_ta.player_twitter_x_url
        ) as player_twitter_x_url,

        coalesce(
            p_ta.player_hand_plays,
            pc_ta.player_hand_plays
        ) as player_hand_plays,

        coalesce(
            p_ta.player_tour_id_name,
            pc_ta.player_tour_id_name,
            p.player_tour_id_name
        ) as player_tour_id_name,

        coalesce(
            p_ta.player_team_cup_id_name,
            pc_ta.player_team_cup_id_name,
            p.player_team_cup_id_name
        ) as player_team_cup_id_name,

        coalesce(
            p_ta.player_wikipedia_url,
            pc_ta.player_wikipedia_url
        ) as player_wikipedia_url,

    from players_tour_values as p
    left join tennisabstract_players as p_ta on p.bk_player = p_ta.bk_player
    left join tennisabstract_players_classic as pc_ta on p.bk_player = pc_ta.bk_player
    left join tennisabstract_matches_players as mp_ta on p.bk_player = mp_ta.bk_player
)

select * from final