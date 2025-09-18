{{
    config(
        materialized='table'
    )
}}

with

tennisabstract_matches as (
    select * from {{ ref('int__web__tennisabstract__matches') }}
),

-- parse out points into own rows
-- parse out point dict values into columns
tennisabstract_matches_points as (
    select
        *,
        
        cast(json_value(point_dict, '$.point_number') as integer) as point_number_in_match,

        -- replace non-breaking space
        replace(
            json_value(point_dict, '$.server'),
            chr(160),
            ' '
        ) as point_server,
        
        -- replace non-standard '-'
        regexp_replace(
            json_value(point_dict, '$.sets'),
            r'[‐-‒–—−]',
            '-'
        ) as set_score_in_match,
        
        -- replace non-standard '-'
        regexp_replace(
            json_value(point_dict, '$.games'),
            r'[‐-‒–—−]',
            '-'
        ) as game_score_in_set,

        -- replace non-standard '-'
        regexp_replace(
            json_value(point_dict, '$.points'),
            r'[‐-‒–—−]',
            '-'
        ) as point_score_in_game,

        -- split by ';' (converts ';{any number of spaces}to ';')
        split(
            regexp_replace(json_value(point_dict, '$.point_description'), r';\s*', ';'),
            ';'
        ) as point_shotlog,

    from tennisabstract_matches,
    unnest(tennisabstract_matches.match_pointlog) as point_dict
),

-- clean invalid score values present in the data
tennisabstract_matches_points_clean_scores as (
    select
        * REPLACE (

            -- game_score_in_set: manually inspect/select/clean rows
            -- make sure the CASE/WHEN is specific enough to the row
            (
                case

                    when match_url = 'https://www.tennisabstract.com/charting/19931107-M-Paris_Masters-SF-Stefan_Edberg-Goran_Ivanisevic.html'
                    and point_number_in_match = 3
                    and set_score_in_match = '0-0'
                    and game_score_in_set = '-0'
                    and point_score_in_game = '15-15'
                    then '0-0'

                    when match_url = 'https://www.tennisabstract.com/charting/20160123-M-Australian_Open-R32-Stan_Wawrinka-Lukas_Rosol.html'
                    and point_number_in_match = 59
                    and set_score_in_match = '1-0'
                    and game_score_in_set = '0-'
                    and point_score_in_game = '30-15'
                    then '0-0'

                    when match_url = 'https://www.tennisabstract.com/charting/20241119-M-Montemar_CH-R32-Francesco_Passaro-Nicolai_Budkov_Kjaer.html'
                    and point_number_in_match = 161
                    and set_score_in_match = '1-1'
                    and game_score_in_set = '-0'
                    and point_score_in_game = '0-0'
                    then '0-0'

                    else game_score_in_set

                end
            ) as game_score_in_set,

            -- point_score_in_game: manually inspect/select/clean rows
            (
                case

                    when game_score_in_set = '6-6'
                        then
                        case
                            when contains_substr(point_score_in_game, 'Jan') then replace(point_score_in_game, 'Jan', '1')
                            when contains_substr(point_score_in_game, 'Feb') then replace(point_score_in_game, 'Feb', '2')
                            when contains_substr(point_score_in_game, 'Mar') then replace(point_score_in_game, 'Mar', '3')
                            when contains_substr(point_score_in_game, 'Apr') then replace(point_score_in_game, 'Apr', '4')
                            when contains_substr(point_score_in_game, 'May') then replace(point_score_in_game, 'May', '5')
                            when contains_substr(point_score_in_game, 'Jun') then replace(point_score_in_game, 'Jun', '6')
                            when contains_substr(point_score_in_game, 'Jul') then replace(point_score_in_game, 'Jul', '7')
                            when contains_substr(point_score_in_game, 'Aug') then replace(point_score_in_game, 'Aug', '8')
                            when contains_substr(point_score_in_game, 'Sep') then replace(point_score_in_game, 'Sep', '9')
                            when contains_substr(point_score_in_game, 'Oct') then replace(point_score_in_game, 'Oct', '10')
                            when contains_substr(point_score_in_game, 'Nov') then replace(point_score_in_game, 'Nov', '11')
                            when contains_substr(point_score_in_game, 'Dec') then replace(point_score_in_game, 'Dec', '12')
                            else point_score_in_game
                        end
                    
                    else point_score_in_game

                end
            ) as point_score_in_game
        ),

    from tennisabstract_matches_points
),

-- get point receiver
tennisabstract_matches_points_point_receiver as (
    select
        *,

        -- get receiver
        -- compare lower case in case weird capitalization in names like 'McHale'
        case
            when lower(point_server) = lower(match_player_one) then match_player_two
            when lower(point_server) = lower(match_player_two) then match_player_one
            else null
        end as point_receiver,

    from tennisabstract_matches_points_clean_scores
),

-- parse out scores
-- scores split by '–' (en dash, versus '-' typical dash)
tennisabstract_matches_points_parse_scores as (
    select
        *,
        
        split(set_score_in_match, '-')[0] as set_score_in_match_server,
        split(set_score_in_match, '-')[1] as set_score_in_match_receiver,
        
        split(game_score_in_set, '-')[0] as game_score_in_set_server,
        split(game_score_in_set, '-')[1] as game_score_in_set_receiver,
        
        split(point_score_in_game, '-')[0] as point_score_in_game_server,
        split(point_score_in_game, '-')[1] as point_score_in_game_receiver,
    
    from tennisabstract_matches_points_point_receiver
),

-- convert scores to int
tennisabstract_matches_points_int_scores as (
    select
        *,
        
        safe_cast(set_score_in_match_server as int) as set_score_in_match_server_int,
        safe_cast(set_score_in_match_receiver as int) as set_score_in_match_receiver_int,
        
        safe_cast(game_score_in_set_server as int) as game_score_in_set_server_int,
        safe_cast(game_score_in_set_receiver as int) as game_score_in_set_receiver_int,
        
        -- convert 'AD' to numeric
        safe_cast(replace(point_score_in_game_server, 'AD', '41') as int) as point_score_in_game_server_int,
        safe_cast(replace(point_score_in_game_receiver, 'AD', '41') as int) as point_score_in_game_receiver_int,
    
    from tennisabstract_matches_points_parse_scores
),

-- add scores
tennisabstract_matches_points_add_scores as (
    select
        *,
        set_score_in_match_server_int + set_score_in_match_receiver_int + 1 as set_number_in_match,
        game_score_in_set_server_int + game_score_in_set_receiver_int + 1 as game_number_in_set,

    from tennisabstract_matches_points_int_scores
),

-- get running counts for determining <match_unit> # within <match_unit>
tennisabstract_matches_points_running_numbers as (
    select
        *,
        dense_rank() over (
            partition by bk_match
            order by set_number_in_match, game_number_in_set
        ) as game_number_in_match,
        row_number() over (
            partition by bk_match, set_number_in_match
            order by game_number_in_set, point_number_in_match
        ) as point_number_in_set,
        row_number() over (
            partition by bk_match, set_number_in_match, game_number_in_set
            order by point_number_in_match
        ) as point_number_in_game
    from tennisabstract_matches_points_add_scores
),

-- get side of court
tennisabstract_matches_points_point_side as (
    select
        *,
        case
            -- determine side based on non-tiebreaker scores
            when point_score_in_game in (
                '0-0', '0-30',
                '15-15', '15-40',
                '30-0', '30-30',
                '40-40'
            ) then 'deuce'
            when point_score_in_game in (
                '0-15', '0-40',
                '15-0', '15-30',
                '30-15', '30-40',
                'AD-40', '40-AD'
            ) then 'ad'
            -- determine side based on tiebreak scores
            when mod(point_score_in_game_server_int + point_score_in_game_receiver_int, 2) = 0 then 'deuce'
            when mod(point_score_in_game_server_int + point_score_in_game_receiver_int, 2) != 0 then 'ad'
        else null
        end as point_side
    from tennisabstract_matches_points_running_numbers
),

-- get point result
tennisabstract_matches_points_point_result as (
    select
        *,

        -- order matters
        case
            when regexp_contains(lower(array_last(point_shotlog)), r'ace') then 'ace'
            when regexp_contains(lower(array_last(point_shotlog)), r'double fault') then 'double fault'
            when regexp_contains(lower(array_last(point_shotlog)), r'service winner') then 'service winner'
            when regexp_contains(lower(array_last(point_shotlog)), r'winner') then 'winner'
            when regexp_contains(lower(array_last(point_shotlog)), r'unforced error') then 'unforced error'
            when regexp_contains(lower(array_last(point_shotlog)), r'forced error') then 'forced error'
            else null
        end as point_result,

    from tennisabstract_matches_points_point_side
),

-- get rally length
tennisabstract_matches_points_rally as (
    select
        *,

        -- calculate number of shots based on point result
        case
            when point_result in ('ace', 'double fault', 'forced error', 'service winner', 'unforced error', 'winner') then array_length(point_shotlog)
            else null
        end as number_of_shots,

        -- calculate rally length based on point result
        case
            when point_result in ('ace', 'service winner', 'winner') then array_length(point_shotlog)
            when point_result in ('double fault', 'forced error', 'unforced error') then array_length(point_shotlog) - 1
            else null
        end as rally_length,

    from tennisabstract_matches_points_point_result
),

-- get point winner
tennisabstract_matches_points_point_winner as (
    select
        *,

        case
            -- when server hit last shot (1st, 3rd, etc.)
            when mod(number_of_shots, 2) != 0 then
                case
                    -- when 'winner'-like shot
                    when point_result in ('ace', 'service winner', 'winner') then point_server
                    -- when 'error'-like shot
                    when point_result in ('double fault', 'forced error', 'unforced error') then point_receiver
                    else null
                end
            
            -- when receiver hit last shot (2nd, 4th, etc.)
            when mod(number_of_shots, 2) = 0 then
                case
                    -- when 'winner'-like shot
                    when point_result in ('ace', 'service winner', 'winner') then point_receiver
                    -- when 'error'-like shot
                    when point_result in ('double fault', 'forced error', 'unforced error') then point_server
                    else null
                end

            else null
        end as point_winner,

    from tennisabstract_matches_points_rally
),

-- get point loser
tennisabstract_matches_points_point_loser as (
    select
        *,

        case
            when point_winner = point_server then point_receiver
            when point_winner = point_receiver then point_server
            else null
        end as point_loser,

    from tennisabstract_matches_points_point_winner
),

-- determine game/break points
tennisabstract_matches_points_game_point_type as (
    select
        *,

        -- determine break point
        case
            when point_score_in_game in (
                '0-40', '15-40', '30-40', '40-AD'
            ) then true
            else false
        end as is_break_point,

        -- determine game point
        case
            when point_score_in_game in (
                '40-0', '40-15', '40-30', 'AD-40'
            ) then true
            else false
        end as is_game_point,

    from tennisabstract_matches_points_point_loser
),

final as (
    select
        {{ generate_bk_point(
            bk_match_col='bk_match',
            point_number_col='point_number_in_match'
        )}} as bk_point,
        bk_match,
        point_number_in_match,
        match_url,
        point_dict,

        {{ generate_bk_game(
            bk_match_col='bk_match',
            game_number_col='game_number_in_match'
        )}} as bk_game,

        {{ generate_bk_set(
            bk_match_col='bk_match',
            set_number_col='set_number_in_match'
        )}} as bk_set,        

        point_server,
        point_receiver,
        {{ generate_bk_player(
            player_name_col='point_server',
            player_gender_col='match_gender'
        ) }} as bk_point_server,
        {{ generate_bk_player(
            player_name_col='point_receiver',
            player_gender_col='match_gender'
        ) }} as bk_point_receiver,

        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_shotlog,
        set_score_in_match_server,
        set_score_in_match_receiver,
        game_score_in_set_server,
        game_score_in_set_receiver,
        point_score_in_game_server,
        point_score_in_game_receiver,
        set_score_in_match_server_int,
        set_score_in_match_receiver_int,
        game_score_in_set_server_int,
        game_score_in_set_receiver_int,
        point_score_in_game_server_int,
        point_score_in_game_receiver_int,
        set_number_in_match,
        game_number_in_set,
        game_number_in_match,
        point_number_in_set,
        point_number_in_game,
        point_side,
        point_result,
        number_of_shots,
        rally_length,
        point_winner,
        point_loser,
        is_break_point,
        is_game_point,
    from tennisabstract_matches_points_game_point_type
)

select * from final