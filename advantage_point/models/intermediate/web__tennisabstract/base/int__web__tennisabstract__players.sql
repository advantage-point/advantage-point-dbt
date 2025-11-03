with

tennisabstract_players as (
    select * from {{ ref('stg__web__tennisabstract__players') }}
    where audit_column__active_flag = true
),

-- create bk_player
tennisabstract_players_bk_player as (
    select
        *,
        {{ generate_bk_player(
            player_name_col='player_name',
            player_gender_col='player_gender'
        )}} as bk_player
    from tennisabstract_players
),

final as (
    select
        bk_player,
        player_name,
        player_gender,
        player_url,
        player_full_name,
        player_last_name,
        player_current_singles_ranking,
        player_peak_singles_ranking,
        player_first_peak_singles_ranking_date,
        player_last_peak_singles_ranking_date,
        {{ generate_bk_date(
            date_col='player_date_of_birth'
        ) }} as bk_player_birth_date,
        player_date_of_birth,
        player_height_in_cm,
        player_hand,
        player_backhand,
        player_country,
        is_player_active,
        player_last_match_played_date,
        player_twitter_handle as player_twitter_x_handle,
        player_current_doubles_ranking,
        player_peak_doubles_ranking,
        player_first_peak_doubles_ranking_date,
        photograph as player_tennisabstract_photograph,
        photograph_credit as player_tennisabstract_photograph_credit,
        photograph_link as player_tennisabstract_photograph_link,
        player_itf_id,
        player_tour_id,
        player_team_cup_id,
        player_wikipedia_id,

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
            photograph,
            '.jpg'
        ) as player_tennisabstract_photograph_url,

        -- create X url
        concat(
            'https://www.x.com/',
            player_twitter_handle
        ) as player_twitter_x_url,

        -- create 'human readable' handedness
        case
            when lower(player_hand) = 'r' then 'right-handed'
            when lower(player_hand) = 'l' then 'left-handed'
            else null
        end as player_hand_plays,

        -- create wikipedia url
        concat(
            'https://en.wikipedia.org/wiki/',
            player_wikipedia_id
        ) as player_wikipedia_url,
    from tennisabstract_players_bk_player
)

select * from final