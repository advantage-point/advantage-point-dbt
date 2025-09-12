with

tennisabstract_players as (
    select * from {{ ref('stg__web__tennisabstract__players') }}
    where audit_column__active_flag = true
),

tennisabstract_players_classic as (
    select * from {{ ref('stg__web__tennisabstract__players_classic') }}
    where audit_column__active_flag = true
),

tennisabstract_matches as (
    select * from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- create player id
tennisabstract_players_player_id as (
    select
        *,
        concat(
            player_name,
            '||',
            player_gender
        ) as player_id
    from tennisabstract_players
),

-- create player id
tennisabstract_players_classic_player_id as (
    select
        *,
        concat(
            player_name,
            '||',
            player_gender
        ) as player_id
    from tennisabstract_players_classic
),

-- get players from match data
tennisabstract_matches_players as (
    select distinct
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

-- create player id
tennisabstract_matches_players_player_id as (
    select
        *,
        concat(
            player_name,
            '||',
            player_gender
        ) as player_id
    from tennisabstract_matches_players
),

-- union players
players_union as (
    select distinct
        *
    from (
        (
            select
                player_id,
                player_name,
                player_gender
            from tennisabstract_players_player_id
        )
        union all
        (
            select
                player_id,
                player_name,
                player_gender
            from tennisabstract_players_classic_player_id
        )
        union all
        (
            select
                player_id,
                player_name,
                player_gender
            from tennisabstract_matches_players_player_id
        )
    ) as p
),

-- join data to players
players_joined as (
    select
        p.player_id,
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
        ) as player_wikipedia_id

    from players_union as p
    left join tennisabstract_players_player_id as p_ta on p.player_id = p_ta.player_id
    left join tennisabstract_players_classic_player_id as pc_ta on p.player_id = pc_ta.player_id
    left join tennisabstract_matches_players_player_id as mp_ta on p.player_id = mp_ta.player_id
),

-- add logic from tennisabstract pages
players_tennisabstract_logic as (
    select
        *,

        case
            when player_backhand = '1' then 'one-handed'
            when player_backhand = '2' then 'two-handed'
            else null
        end as player_backhand_plays,

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

        concat(
            'https://www.x.com/',
            player_twitter_x_handle
        ) as player_twitter_x_url,

        case
            when lower(player_hand) = 'r' then 'right-handed'
            when lower(player_hand) = 'l' then 'left-handed'
            else null
        end as player_hand_plays,

        case
            when player_gender = 'M' then 'ATP'
            when player_gender = 'W' then 'WTA'
            else null
        end as player_tour_id_name,

        case
            when player_gender = 'M' then 'Davis Cup'
            when player_gender = 'W' then 'Billie Jean King Cup'
            else null
        end as player_team_cup_id_name,

        concat(
            'https://en.wikipedia.org/wiki/',
            player_wikipedia_id
        ) as player_wikipedia_url

    from players_joined
)

select * from players_tennisabstract_logic