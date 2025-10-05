{{
    config(
        materialized='table',
        cluster_by=['bk_match', 'bk_set', 'bk_game',],
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
                    and point_number_in_match between 161 and 165
                    and set_score_in_match = '1-1'
                    and point_server = 'Nicolai Budkov Kjaer'
                    then '2-0'

                    when match_url = 'https://www.tennisabstract.com/charting/20241119-M-Montemar_CH-R32-Francesco_Passaro-Nicolai_Budkov_Kjaer.html'
                    and point_number_in_match between 166 and 169
                    and set_score_in_match = '1-1'
                    and point_server = 'Francesco Passaro'
                    then '0-3'

                    when match_url = 'https://www.tennisabstract.com/charting/20241119-M-Montemar_CH-R32-Francesco_Passaro-Nicolai_Budkov_Kjaer.html'
                    and point_number_in_match between 170 and 174
                    and set_score_in_match = '1-1'
                    and point_server = 'Nicolai Budkov Kjaer'
                    then '4-0'

                    when match_url = 'https://www.tennisabstract.com/charting/20241119-M-Montemar_CH-R32-Francesco_Passaro-Nicolai_Budkov_Kjaer.html'
                    and point_number_in_match between 175 and 178
                    and set_score_in_match = '1-1'
                    and point_server = 'Francesco Passaro'
                    then '0-5'

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

-- create bks
tennisabstract_matches_points_bks as (
    select
        *,

        {{ generate_bk_point(
            bk_match_col='bk_match',
            point_number_col='point_number_in_match'
        )}} as bk_point,

        {{ generate_bk_game(
            bk_match_col='bk_match',
            game_number_col='game_number_in_match'
        )}} as bk_game,

        {{ generate_bk_set(
            bk_match_col='bk_match',
            set_number_col='set_number_in_match'
        )}} as bk_set,

        {{ generate_bk_player(
            player_name_col='point_server',
            player_gender_col='match_gender'
        ) }} as bk_point_server,

        {{ generate_bk_player(
            player_name_col='point_receiver',
            player_gender_col='match_gender'
        ) }} as bk_point_receiver,

    from tennisabstract_matches_points_rally
),


-- get point winner from rally result
tennisabstract_matches_points_point_winner_result as (
    select
        *,

        case
            -- when server hit last shot (1st, 3rd, etc.)
            when mod(number_of_shots, 2) != 0 then
                case
                    -- when 'winner'-like shot
                    when point_result in ('ace', 'service winner', 'winner') then bk_point_server
                    -- when 'error'-like shot
                    when point_result in ('double fault', 'forced error', 'unforced error') then bk_point_receiver
                    else null
                end
            
            -- when receiver hit last shot (2nd, 4th, etc.)
            when mod(number_of_shots, 2) = 0 then
                case
                    -- when 'winner'-like shot
                    when point_result in ('ace', 'service winner', 'winner') then bk_point_receiver
                    -- when 'error'-like shot
                    when point_result in ('double fault', 'forced error', 'unforced error') then bk_point_server
                    else null
                end

            else null
        end as bk_point_winner_result,

    from tennisabstract_matches_points_bks
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

    from tennisabstract_matches_points_point_winner_result
),

-- calculate player scores
tennisabstract_matches_points_players as (
    select
        *,

        -- player one scores
        lpad(
            (
                case
                    when bk_point_server = bk_match_player_one then set_score_in_match_server
                    when bk_point_receiver = bk_match_player_one then set_score_in_match_receiver
                    else null
                end
            ),
            2,
            '0'
        )  as set_score_in_match_player_one,
        case
            when bk_point_server = bk_match_player_one then set_score_in_match_server_int
            when bk_point_receiver = bk_match_player_one then set_score_in_match_receiver_int
            else null
        end as set_score_in_match_player_one_int,
        lpad(
            (
                case
                    when bk_point_server = bk_match_player_one then game_score_in_set_server
                    when bk_point_receiver = bk_match_player_one then game_score_in_set_receiver
                    else null
                end
            ),
            4,
            '0'
        ) as game_score_in_set_player_one,
        case
            when bk_point_server = bk_match_player_one then game_score_in_set_server_int
            when bk_point_receiver = bk_match_player_one then game_score_in_set_receiver_int
            else null
        end as game_score_in_set_player_one_int,
        lpad(
            cast(
                (
                    case
                        when bk_point_server = bk_match_player_one then point_score_in_game_server_int
                        when bk_point_receiver = bk_match_player_one then point_score_in_game_receiver_int
                        else null
                    end 
                )
            as string),
            4,
            '0'
        ) as point_score_in_game_player_one, -- use the 'int' column since 'AD' recasted
        case
            when bk_point_server = bk_match_player_one then point_score_in_game_server_int
            when bk_point_receiver = bk_match_player_one then point_score_in_game_receiver_int
            else null
        end as point_score_in_game_player_one_int,
        
        -- player 2 scores
        lpad(
            (
                case
                    when bk_point_server = bk_match_player_two then set_score_in_match_server
                    when bk_point_receiver = bk_match_player_two then set_score_in_match_receiver
                    else null
                end
            ),
            2,
            '0'
        )  as set_score_in_match_player_two,
        case
            when bk_point_server = bk_match_player_two then set_score_in_match_server_int
            when bk_point_receiver = bk_match_player_two then set_score_in_match_receiver_int
            else null
        end as set_score_in_match_player_two_int,
        lpad(
            (
                case
                    when bk_point_server = bk_match_player_two then game_score_in_set_server
                    when bk_point_receiver = bk_match_player_two then game_score_in_set_receiver
                    else null
                end
            ),
            4,
            '0'
        ) as game_score_in_set_player_two,
        case
            when bk_point_server = bk_match_player_two then game_score_in_set_server_int
            when bk_point_receiver = bk_match_player_two then game_score_in_set_receiver_int
            else null
        end as game_score_in_set_player_two_int,
        lpad(
            cast(
                (
                    case
                        when bk_point_server = bk_match_player_two then point_score_in_game_server_int
                        when bk_point_receiver = bk_match_player_two then point_score_in_game_receiver_int
                        else null
                    end 
                )
            as string),
            4,
            '0'
        ) as point_score_in_game_player_two, -- use the 'int' column since 'AD' recasted
        case
            when bk_point_server = bk_match_player_two then point_score_in_game_server_int
            when bk_point_receiver = bk_match_player_two then point_score_in_game_receiver_int
            else null
        end as point_score_in_game_player_two_int,
        
    from tennisabstract_matches_points_game_point_type
),

-- concatenate player scores
tennisabstract_matches_points_players_concat as (
    select
        *,

        -- player one
        concat(
            set_score_in_match_player_one,
            '-',
            game_score_in_set_player_one
        ) as game_score_in_match_full_player_one,
        concat(
            set_score_in_match_player_one,
            '-',
            game_score_in_set_player_one,
            '-',
            point_score_in_game_player_one
        ) as point_score_in_match_full_player_one,

        -- player two
        concat(
            set_score_in_match_player_two,
            '-',
            game_score_in_set_player_two
        ) as game_score_in_match_full_player_two,
        concat(
            set_score_in_match_player_two,
            '-',
            game_score_in_set_player_two,
            '-',
            point_score_in_game_player_two
        ) as point_score_in_match_full_player_two,

    from tennisabstract_matches_points_players
),

-- get 'next point' values (including point winner)
tennisabstract_matches_points_next_point as (
    select
        p.*,

        -- calculate point winner based on next point score
        case
            when p_next.point_score_in_match_full_player_one > p.point_score_in_match_full_player_one then p.bk_match_player_one
            when p_next.point_score_in_match_full_player_two > p.point_score_in_match_full_player_two then p.bk_match_player_two
            else null
        end as bk_point_winner_next_point,

    from tennisabstract_matches_points_players_concat as p
    left join tennisabstract_matches_points_players_concat as p_next on 1=1
        and p.bk_match = p_next.bk_match
        and p.point_number_in_match + 1 = p_next.point_number_in_match
),

-- calculate point winner
tennisabstract_matches_points_point_winner as (
    select
        *,

        -- calculate point winner based on current winner columns
        coalesce(bk_point_winner_next_point, bk_point_winner_result) as bk_point_winner,
    
    from tennisabstract_matches_points_next_point
),


-- get point loser
tennisabstract_matches_points_point_loser as (
    select
        *,

        case
            when bk_point_winner = bk_point_server then bk_point_receiver
            when bk_point_winner = bk_point_receiver then bk_point_server
            else null
        end as bk_point_loser,

    from tennisabstract_matches_points_point_winner
),

-- get last point in match units (game, sets)
tennisabstract_matches_points_last_point as (
    select
        *,

        point_number_in_match = max(point_number_in_match) over (partition by bk_match order by bk_set, bk_game) as is_last_point_in_game,
        point_number_in_match = max(point_number_in_match) over (partition by bk_match order by bk_set) as is_last_point_in_set,

    from tennisabstract_matches_points_point_loser
),


final as (
    select
        bk_point,
        bk_match,
        point_number_in_match,
        match_url,
        point_dict,

        bk_game,
        bk_set,        

        bk_point_server,
        bk_point_receiver,
        bk_point_winner_result,

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
        is_break_point,
        is_game_point,
        set_score_in_match_player_one,
        set_score_in_match_player_one_int,
        set_score_in_match_player_two,
        set_score_in_match_player_two_int,
        game_score_in_set_player_one,
        game_score_in_set_player_one_int,
        game_score_in_set_player_two,
        game_score_in_set_player_two_int,
        point_score_in_game_player_one,
        point_score_in_game_player_one_int,
        point_score_in_game_player_two,
        point_score_in_game_player_two_int,

        game_score_in_match_full_player_one,
        point_score_in_match_full_player_one,
        game_score_in_match_full_player_two,
        point_score_in_match_full_player_two,

        bk_point_winner_next_point,
        bk_point_winner,
        bk_point_loser,

        is_last_point_in_game,
        is_last_point_in_set,


    from tennisabstract_matches_points_last_point
)

select * from final