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

tournaments as (
    select * from {{ ref('int__tournaments') }}
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
            regexp_replace(
                -- replace mid-string result phrases (e.g., ',winner.' or ', service winner.')
                regexp_replace(
                    lower(json_value(point_dict, '$.point_description')),  -- normalize casing
                    --  r',winner\.[A-Za-z]',
                    r',(service winner|winner|unforced error|forced error|double fault|ace)\.([a-z])',
                    ';\\2'
                ),
                r';\s*',
                ';'
            ),
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

-- apply another layer of cleaning
-- mostly to fix 'swapped scores'
tennisabstract_matches_points_swap_scores as (
    select
        * replace (

            -- point_score_in_game: manually inspect/select/clean rows
            -- make sure the CASE/WHEN is specific enough to the row
            (
                case

                    when match_url = 'https://www.tennisabstract.com/charting/20130818-M-Cincinnati_Masters-F-John_Isner-Rafael_Nadal.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 71 and 88 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (74, 76, 78, 79, 80, 82, 84, 86, 88)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 149 and 158 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (152, 153, 154, 155, 156, 157, 158)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20131005-M-Tokyo-SF-Nicolas_Almagro-Juan_Martin_Del_Potro.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 68 and 83 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (71, 73, 75, 77, 79, 81, 83)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 143 and 150 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (147, 148, 149)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20130501-M-Munich-R16-Dmitry_Tursunov-Alexandr_Dolgopolov.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 63 and 71 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (65, 66, 67, 68, 69, 70, 71)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 153 and 162 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (156, 158, 159, 160, 161, 162)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20130908-W-US_Open-F-Victoria_Azarenka-Serena_Williams.html' then
                        case
                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 141 and 154 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (144, 145, 146, 148, 150, 151, 152, 152, 154)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20140526-W-Roland_Garros-R128-Kirsten_Flipkens-Danka_Kovinic.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 81 and 94 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (84, 85, 86, 87, 88, 89, 90, 92, 94)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20121112-M-Tour_Finals-F-Roger_Federer-Novak_Djokovic.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 87 and 100 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (90, 92, 94, 96, 98, 100)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20130818-W-Cincinnati-F-Victoria_Azarenka-Serena_Williams.html' then
                        case
                            -- 3rd set TB
                            when 1=1
                                and point_number_in_match between 193 and 206 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (196, 197, 198, 199, 200, 202, 204, 206)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20140210-M-Memphis-R32-Nick_Kyrgios-Tim_Smyczek.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 71 and 82 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (74, 76, 78, 79, 80, 81, 82)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20131104-M-Tour_Finals-RR-Richard_Gasquet-Juan_Martin_Del_Potro.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 61 and 71 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (64, 66, 68, 69, 70, 71)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20150131-W-Australian_Open-F-Maria_Sharapova-Serena_Williams.html' then
                        case
                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 129 and 140 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (132, 133, 134, 135, 136, 137, 138, 139, 140)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20151031-W-Singapore-SF-Agnieszka_Radwanska-Garbine_Muguruza.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 73 and 84 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (76, 77, 78, 79, 80, 82, 83, 84)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20120129-M-Australian_Open-F-Novak_Djokovic-Rafael_Nadal.html' then
                        case
                            -- 4th set TB
                            when 1=1
                                and point_number_in_match between 285 and 296 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (288, 290, 292, 293, 296)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20130426-M-Barcelona-QF-Tomas_Berdych-Tommy_Robredo.html' then
                        case
                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 122 and 133 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (125, 127, 128, 129, 130, 131, 132, 133)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20150221-W-Dubai-F-Karolina_Pliskova-Simona_Halep.html' then
                        case
                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 139 and 149 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (142, 144, 146, 148, 149)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20070907-W-US_Open-SF-Venus_Williams-Justine_Henin.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 73 and 81 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (76, 77, 78, 79, 80, 81)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20130831-W-US_Open-R32-Alize_Cornet-Victoria_Azarenka.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 80 and 88 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (82, 83, 84, 85, 86, 87, 88)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20160323-W-Miami-R128-Timea_Babos-Anna_Tatishvili.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 85 and 93 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (88, 89, 90, 91, 92, 93)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20140423-M-Barcelona-R32-Albert_Ramos-Rafael_Nadal.html' then
                        case
                            -- 1st set TB
                            when 1=1
                                and point_number_in_match between 68 and 76 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (71, 73, 74, 76)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/20131025-M-Basel-QF-Grigor_Dimitrov-Roger_Federer.html' then
                        case
                            -- 2nd set TB
                            when 1=1
                                and point_number_in_match between 136 and 144 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (139, 140, 141, 142, 143)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    when match_url = 'https://www.tennisabstract.com/charting/19700913-M-US_Open-F-Tony_Roche-Ken_Rosewall.html' then
                        case
                            -- 3rd set TB
                            when 1=1
                                and point_number_in_match between 184 and 190 -- need to use point number since the set score flips 
                                and game_score_in_set = '6-6'
                                and point_number_in_match in (185)
                            then array_to_string(array_reverse(split(point_score_in_game, '-')), '-')

                            else point_score_in_game

                        end

                    else point_score_in_game
                end
            ) as point_score_in_game
        ),
    from tennisabstract_matches_points_clean_scores
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

    from tennisabstract_matches_points_swap_scores
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
        
    from tennisabstract_matches_points_point_winner_result
),

-- join to tournaments to get formats
tennisabstract_matches_points_tournament_formats as (
    select
        p.*,

        t.best_of_sets,
        t.sets_to_win,
        t.games_per_set,
        t.tiebreak_trigger_game,
        t.tiebreak_points,
        t.final_set_tiebreak_trigger_game,
        t.final_set_tiebreak_points,
        t.is_ad_scoring,

        -- is set the last set in the match
        p.set_number_in_match = t.best_of_sets as is_final_set,

    from tennisabstract_matches_points_players as p
    left join tournaments as t on p.bk_match_tournament = t.bk_tournament
),

-- determine tiebreak flags for games
tennisabstract_matches_points_game_tiebreak_flags as (
    select
        *,

        -- determine if game is tiebreak game
        case
            -- when in final set
            when 1=1
                and is_final_set = true
                and game_score_in_set = final_set_tiebreak_trigger_game
            then true

            -- when not in final set
            when 1=1
                and is_final_set = false
                and game_score_in_set = tiebreak_trigger_game
            then true

            else false
        
        end as is_tiebreak_game,

        -- determine if 
    from tennisabstract_matches_points_tournament_formats
),

-- determine set tiebreak flags
tennisabstract_matches_points_set_tiebreak_flags as (
    select
        *,

        -- check if any values within set are 'tiebreak games'
        max(cast(is_tiebreak_game as int)) over (partition by bk_set) = 1 as is_tiebreak_set,

    from tennisabstract_matches_points_game_tiebreak_flags
),

-- determine point flags
tennisabstract_matches_points_game_point_flags as (
    select
        *,

        -- determine game point for player one
        case

            -- when in tiebreak --> false
            when is_tiebreak_game = true then false

            -- when no ad scoring --> true when player is serving at 'deuce'
            when 1=1
                and is_ad_scoring = false
                and bk_point_server = bk_match_player_one
                and point_score_in_game = '40-40'
            then true

            -- when ad scoring --> true when player is serving up '40-{something}' or has advantage
            when 1=1
                and is_ad_scoring = true
                and bk_point_server = bk_match_player_one
                and point_score_in_game_player_one_int >= 40
                and point_score_in_game_player_one_int > point_score_in_game_player_two_int
            then true

            else false

        end as is_game_point_player_one,

        -- determine game point for player two
        case

            -- when in tiebreak --> false
            when is_tiebreak_game = true then false

            -- when no ad scoring --> true when player is serving at 'deuce'
            when 1=1
                and is_ad_scoring = false
                and bk_point_server = bk_match_player_two
                and point_score_in_game = '40-40'
            then true

            -- when ad scoring --> true when player is serving up '40-{something}' or has advantage
            when 1=1
                and is_ad_scoring = true
                and bk_point_server = bk_match_player_two
                and point_score_in_game_player_two_int >= 40
                and point_score_in_game_player_two_int > point_score_in_game_player_one_int
            then true

            else false

        end as is_game_point_player_two,

        -- determine break point for player one
        case

            -- when in tiebreak --> false
            when is_tiebreak_game = true then false

            -- when no ad scoring --> true when player is receiving at 'deuce'
            when 1=1
                and is_ad_scoring = false
                and bk_point_receiver = bk_match_player_one
                and point_score_in_game = '40-40'
            then true

            -- when ad scoring --> true when player is receiving up '{something}-40' or has advantage
            when 1=1
                and is_ad_scoring = true
                and bk_point_receiver = bk_match_player_one
                and point_score_in_game_player_one_int >= 40
                and point_score_in_game_player_one_int > point_score_in_game_player_two_int
            then true

            else false

        end as is_break_point_player_one,

        -- determine break point for player two
        case

            -- when in tiebreak --> false
            when is_tiebreak_game = true then false

            -- when no ad scoring --> true when player is receiving at 'deuce'
            when 1=1
                and is_ad_scoring = false
                and bk_point_receiver = bk_match_player_two
                and point_score_in_game = '40-40'
            then true

            -- when ad scoring --> true when player is receiving up '{something}-40' or has advantage
            when 1=1
                and is_ad_scoring = true
                and bk_point_receiver = bk_match_player_two
                and point_score_in_game_player_two_int >= 40
                and point_score_in_game_player_two_int > point_score_in_game_player_one_int
            then true

            else false

        end as is_break_point_player_two,

    from tennisabstract_matches_points_set_tiebreak_flags
),

-- determine set point flags
tennisabstract_matches_points_game_set_flags as (
    select
        *,

        -- determine set point for player one
        case

            -- when invalid or undefined format --> skip logic
            when best_of_sets is null or games_per_set is null then false

            -- tiebreak scenarios (regular or final sets)
            when is_tiebreak_game = true then
                
                case

                    -- when final set tiebreak
                    when 1=1
                        and is_final_set = true -- in final set
                        and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
                        and final_set_tiebreak_points is not null -- format explicitly defines final set TB points
                        and point_score_in_game_player_one_int >= final_set_tiebreak_points - 1 -- one point away from target
                        and point_score_in_game_player_one_int > point_score_in_game_player_two_int -- leading
                    then true

                    -- when regular (non-final) set tiebreak
                    when 1=1
                        and is_final_set = false -- not final set
                        and tiebreak_trigger_game is not null -- format defines regular set TB
                        and tiebreak_points is not null -- format defines regular set TB points
                        and point_score_in_game_player_one_int >= tiebreak_points - 1 -- one point away from target
                        and point_score_in_game_player_one_int > point_score_in_game_player_two_int -- leading
                    then true

                    -- when 'extended' tiebreak (no limit or missing config)
                    when 1=1
                        and (tiebreak_points is null or final_set_tiebreak_points is null) -- undefined upper limit
                        and point_score_in_game_player_one_int - point_score_in_game_player_two_int = 1 -- leading by 1
                    then true

                    else false

                end

            -- non-tiebreak scenarios
            when is_tiebreak_game = false then

                case

                    -- normal (non-final) sets
                    when 1=1
                        and is_final_set = false -- not last set
                        and game_score_in_set_player_one_int = games_per_set - 1 -- one game away
                        and game_score_in_set_player_one_int > game_score_in_set_player_two_int -- leading
                        and (is_game_point_player_one = true or is_break_point_player_one = true) -- point could close out game
                    then true

                    -- when final set with standard or super tiebreak
                    when 1=1
                        and is_final_set = true -- in final set
                        and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
                        and game_score_in_set_player_one_int = games_per_set - 1 -- one game away
                        and game_score_in_set_player_one_int > game_score_in_set_player_two_int -- leading
                        and (is_game_point_player_one = true or is_break_point_player_one = true) -- point could close out game
                    then true

                    -- when advantage final set (no tiebreak)
                    when 1=1
                        and is_final_set = true -- in final set
                        and (final_set_tiebreak_trigger_game is null or final_set_tiebreak_points is null) -- no TB defined
                        and (game_score_in_set_player_one_int - game_score_in_set_player_two_int = 1) -- leading by 1 game
                        and (is_game_point_player_one = true or is_break_point_player_one = true) -- point could close out game
                    then true 

                    else false

                end

            else false

        end as is_set_point_player_one,

        -- determine set point for player two
        case

            -- when invalid or undefined format --> skip logic
            when best_of_sets is null or games_per_set is null then false

            -- tiebreak scenarios (regular or final sets)
            when is_tiebreak_game = true then
                
                case

                    -- when final set tiebreak
                    when 1=1
                        and is_final_set = true -- in final set
                        and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
                        and final_set_tiebreak_points is not null -- format explicitly defines final set TB points
                        and point_score_in_game_player_two_int >= final_set_tiebreak_points - 1 -- one point away from target
                        and point_score_in_game_player_two_int > point_score_in_game_player_one_int -- leading
                    then true

                    -- when regular (non-final) set tiebreak
                    when 1=1
                        and is_final_set = false -- not final set
                        and tiebreak_trigger_game is not null -- format defines regular set TB
                        and tiebreak_points is not null -- format defines regular set TB points
                        and point_score_in_game_player_two_int >= tiebreak_points - 1 -- one point away from target
                        and point_score_in_game_player_two_int > point_score_in_game_player_one_int -- leading
                    then true

                    -- when 'extended' tiebreak (no limit or missing config)
                    when 1=1
                        and (tiebreak_points is null or final_set_tiebreak_points is null) -- undefined upper limit
                        and point_score_in_game_player_two_int - point_score_in_game_player_one_int = 1 -- leading by 1
                    then true

                    else false

                end

            -- non-tiebreak scenarios
            when is_tiebreak_game = false then

                case

                    -- normal (non-final) sets
                    when 1=1
                        and is_final_set = false -- not last set
                        and game_score_in_set_player_two_int = games_per_set - 1 -- one game away
                        and game_score_in_set_player_two_int > game_score_in_set_player_one_int -- leading
                        and (is_game_point_player_two = true or is_break_point_player_two = true) -- point could close out game
                    then true

                    -- when final set with standard or super tiebreak
                    when 1=1
                        and is_final_set = true -- in final set
                        and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
                        and game_score_in_set_player_two_int = games_per_set - 1 -- one game away
                        and game_score_in_set_player_two_int > game_score_in_set_player_one_int -- leading
                        and (is_game_point_player_two = true or is_break_point_player_two = true) -- point could close out game
                    then true

                    -- when advantage final set (no tiebreak)
                    when 1=1
                        and is_final_set = true -- in final set
                        and (final_set_tiebreak_trigger_game is null or final_set_tiebreak_points is null) -- no TB defined
                        and (game_score_in_set_player_two_int - game_score_in_set_player_one_int = 1) -- leading by 1 game
                        and (is_game_point_player_two = true or is_break_point_player_two = true) -- point could close out game
                    then true 

                    else false

                end

            else false

        end as is_set_point_player_two,

        -- get cumulative sets won by player one
        max(set_score_in_match_player_one_int) over (
            partition by bk_match
            order by point_number_in_match
            rows between unbounded preceding and current row
        ) as cumulative_set_score_in_match_player_one_int,

        -- get cumulative sets won by player two
        max(set_score_in_match_player_two_int) over (
            partition by bk_match
            order by point_number_in_match
            rows between unbounded preceding and current row
        ) as cumulative_set_score_in_match_player_two_int,

    from tennisabstract_matches_points_game_point_flags
),

-- determine match point flags
tennisabstract_matches_points_game_match_flags as (
    select
        *,

        -- determine match point for player one
        case

            -- when invalid or undefined format --> skip
            when (best_of_sets is null or sets_to_win is null) then false

            -- when player has reached required sets to win --> skip
            when cumulative_set_score_in_match_player_one_int >= sets_to_win then false

            -- when player is one set away and has set point
            when 1=1
                and cumulative_set_score_in_match_player_one_int = sets_to_win - 1 -- 1 set away
                and is_set_point_player_one -- set point
            then true

            else false

        end as is_match_point_player_one,

        -- determine match point for player two
        case

            -- when invalid or undefined format --> skip
            when (best_of_sets is null or sets_to_win is null) then false

            -- when player has reached required sets to win --> skip
            when cumulative_set_score_in_match_player_two_int >= sets_to_win then false

            -- when player is one set away and has set point
            when 1=1
                and cumulative_set_score_in_match_player_two_int = sets_to_win - 1  -- 1 set away
                and is_set_point_player_two                                         -- set point
            then true

            else false

        end as is_match_point_player_two,

    from tennisabstract_matches_points_game_set_flags
),

-- concatenate player scores
tennisabstract_matches_points_players_concat as (
    select
        *,

        -- player one
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
            game_score_in_set_player_two,
            '-',
            point_score_in_game_player_two
        ) as point_score_in_match_full_player_two,

    from tennisabstract_matches_points_game_match_flags
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
        coalesce(bk_point_winner_result, bk_point_winner_next_point) as bk_point_winner,
    
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

        point_number_in_match = max(point_number_in_match) over (partition by bk_match, bk_set, bk_game) as is_last_point_in_game,
        point_number_in_match = max(point_number_in_match) over (partition by bk_match, bk_set) as is_last_point_in_set,

    from tennisabstract_matches_points_point_loser
),

-- flag row data quality
tennisabstract_matches_points_quality as (
    select
        *,

        -- flag rows
        case
            when 1=1
                and bk_point_winner_result = bk_point_winner_next_point
                and point_result is not null
            then true
            else false
        end as is_quality_point,

    from tennisabstract_matches_points_last_point
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

        is_tiebreak_game,
        
        is_tiebreak_set,
        is_final_set,

        is_game_point_player_one,
        is_game_point_player_two,
        is_break_point_player_one,
        is_break_point_player_two,

        is_set_point_player_one,
        is_set_point_player_two,
        cumulative_set_score_in_match_player_one_int,
        cumulative_set_score_in_match_player_two_int,

        is_match_point_player_one,
        is_match_point_player_two,

        point_score_in_match_full_player_one,
        point_score_in_match_full_player_two,

        bk_point_winner_next_point,
        bk_point_winner,
        bk_point_loser,

        is_last_point_in_game,
        is_last_point_in_set,

        is_quality_point,


    from tennisabstract_matches_points_quality
)

select * from final