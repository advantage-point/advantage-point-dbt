with

tennisabstract_players as (
    select
        {{ generate_bk_player(
            player_name_col='player_name',
            player_gender_col='player_gender'
        )}} as bk_player,
        *
    from {{ ref('stg__web__tennisabstract__players') }}
    where audit_column__active_flag = true
),

tennisabstract_players_classic as (
    select
        *,
        {{ generate_bk_player(
            player_name_col='player_name',
            player_gender_col='player_gender'
        )}} as bk_player
    from {{ ref('stg__web__tennisabstract__players_classic') }}
    where audit_column__active_flag = true
),

tennisabstract_matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
),

-- get players from match data
tennisabstract_matches_players as (
    select distinct
        {{ generate_bk_player(
            player_name_col='player_name',
            player_gender_col='player_gender'
        ) }} as bk_player,
        *
    from (
        (
            select
                match_player_one as player_name,
                match_gender as player_gender
            from tennisabstract_matches
        )
        union all
        (
            select
                match_player_two as player_name,
                match_gender as player_gender
            from tennisabstract_matches
        )
    ) as p
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

-- join data to players
players_joined as (
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
            p_ta.player_twitter_handle,
            pc_ta.player_twitter_handle
        ) as player_twitter_handle,

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
            p_ta.photograph,
            pc_ta.photograph
        ) as player_tennisabstract_photograph,

        coalesce(
            p_ta.photograph_credit,
            pc_ta.photograph_credit
        ) as player_tennisabstract_photograph_credit,

        coalesce(
            p_ta.photograph_link,
            pc_ta.photograph_link
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

    from players_union as p
    left join tennisabstract_players as p_ta on p.bk_player = p_ta.bk_player
    left join tennisabstract_players_classic as pc_ta on p.bk_player = pc_ta.bk_player
    left join tennisabstract_matches_players as mp_ta on p.bk_player = mp_ta.bk_player
),

-- create additional fields on top of the coalesced ones
players_coalesce_calc as (
    select
        *,

        -- create 'human readable' backhand
        case
            when player_backhand = '1' then 'one-handed'
            when player_backhand = '2' then 'two-handed'
            else null
        end as player_backhand_plays,

        -- create TA photo url
        concat(
            'https://www.tennisabstract.com/photos/',
            replace(
                lower(player_full_name),
                ' ',
                '_'
            ),
            '-',
            player_tennisabstract_photograph,
            '.jpg'
        ) as player_tennisabstract_photograph_url,

        -- create twitter/X url
        concat(
            'https://www.x.com/',
            player_twitter_handle
        ) as player_twitter_url,

        -- create 'human readable' handedness
        case
            when lower(player_hand) = 'r' then 'right-handed'
            when lower(player_hand) = 'l' then 'left-handed'
            else null
        end as player_hand_plays,

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

        -- create wikipedia url
        concat(
            'https://en.wikipedia.org/wiki/',
            player_wikipedia_id
        ) as player_wikipedia_url,

        {{ generate_bk_date(
            date_col='player_first_peak_singles_ranking_date'
        ) }} as bk_player_first_peak_singles_ranking_date,

        {{ generate_bk_date(
            date_col='player_last_peak_singles_ranking_date'
        ) }} as bk_player_last_peak_singles_ranking_date,
        
        {{ generate_bk_date(
            date_col='player_date_of_birth'
        ) }} as bk_player_date_of_birth,

        {{ generate_bk_date(
            date_col='player_last_match_played_date'
        ) }} as bk_player_last_match_played_date,

        {{ generate_bk_date(
            date_col='player_first_peak_doubles_ranking_date'
        ) }} as bk_player_first_peak_doubles_ranking_date,

    from players_joined
),

final as (
    select
        bk_player,
        player_name,
        player_gender,

        player_full_name,
        player_last_name,
        player_current_singles_ranking,
        player_peak_singles_ranking,
        bk_player_first_peak_singles_ranking_date,
        player_first_peak_singles_ranking_date,
        bk_player_last_peak_singles_ranking_date,
        player_last_peak_singles_ranking_date,
        bk_player_date_of_birth,
        player_date_of_birth,
        player_height_in_cm,
        player_hand,
        player_backhand,
        player_country,
        is_player_active,
        bk_player_last_match_played_date,
        player_last_match_played_date,
        player_twitter_handle,
        player_current_doubles_ranking,
        player_peak_doubles_ranking,
        bk_player_first_peak_doubles_ranking_date,
        player_first_peak_doubles_ranking_date,
        player_tennisabstract_photograph,
        player_tennisabstract_photograph_credit,
        player_tennisabstract_photograph_link,
        player_itf_id,
        player_tour_id,
        player_team_cup_id,
        player_wikipedia_id,
        player_backhand_plays,
        player_tennisabstract_photograph_url,
        player_twitter_url,
        player_hand_plays,
        player_tour_id_name,
        player_team_cup_id_name,
        player_wikipedia_url,

    from players_coalesce_calc
)

select * from final


        
