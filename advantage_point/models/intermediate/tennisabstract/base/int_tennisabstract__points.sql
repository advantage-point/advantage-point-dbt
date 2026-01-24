-- materialized as table:
    -- points are unnested (1 match = 100's of points)
    -- referenced in multiple downstream models
{{
    config(
        materialized='table',
        cluster_by=['bk_match',],
    )
}}

with

tennisabstract_matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
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

final as (
    select
        {{ generate_bk_point(
            bk_match_col='bk_match',
            point_number_col='point_number_in_match'
        )}} as bk_point,
        point_server,
        bk_match,
        match_url,
        point_number_in_match,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_shotlog,

    from tennisabstract_matches_points_swap_scores
)

select * from final