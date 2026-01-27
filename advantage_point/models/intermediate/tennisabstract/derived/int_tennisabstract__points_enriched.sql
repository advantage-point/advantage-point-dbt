-- materialized as table
    -- referenced in multiple downstream models
{{
    config(
        materialized='table',
        cluster_by=['bk_match', 'bk_set', 'bk_game',],
    )
}}

with

tennisabstract_points as (
    select * from {{ ref('int_tennisabstract__points') }}
),

tennisabstract_matches as (
    select * from {{ ref('int_tennisabstract__matches') }}
),

-- get point server and receiver
tennisabstract_point_players as (
    select
        p.*,

        -- get bk server
        -- compare lower case in case weird capitalization in names like 'McHale'
        case
            when lower(p.point_server) = lower(m.match_player_one) then m.bk_match_player_one
            when lower(p.point_server) = lower(m.match_player_two) then m.bk_match_player_two
            else null
        end as bk_point_server,
            
        -- get bk receiver
        -- compare lower case in case weird capitalization in names like 'McHale'
        case
            when lower(p.point_server) = lower(m.match_player_one) then m.bk_match_player_two
            when lower(p.point_server) = lower(m.match_player_two) then m.bk_match_player_one
            else null
        end as bk_point_receiver,

        m.bk_match_player_one as bk_point_player_one,
        m.bk_match_player_two as bk_point_player_two,

    from tennisabstract_points as p
    left join tennisabstract_matches as m on p.bk_match = m.bk_match
),

-- parse out scores
-- scores split by '–' (en dash, versus '-' typical dash)
tennisabstract_points_parse_scores as (
    select
        *,
        
        split(set_score_in_match, '-')[0] as set_score_in_match_server,
        split(set_score_in_match, '-')[1] as set_score_in_match_receiver,
        
        split(game_score_in_set, '-')[0] as game_score_in_set_server,
        split(game_score_in_set, '-')[1] as game_score_in_set_receiver,
        
        split(point_score_in_game, '-')[0] as point_score_in_game_server,
        split(point_score_in_game, '-')[1] as point_score_in_game_receiver,
    
    from tennisabstract_point_players
),

-- convert scores
tennisabstract_points_convert_scores as (
    select
        *,
        
        safe_cast(set_score_in_match_server as int) as set_score_in_match_server_int,
        safe_cast(set_score_in_match_receiver as int) as set_score_in_match_receiver_int,
        
        safe_cast(game_score_in_set_server as int) as game_score_in_set_server_int,
        safe_cast(game_score_in_set_receiver as int) as game_score_in_set_receiver_int,
        
        -- convert 'AD' to numeric
        safe_cast(replace(point_score_in_game_server, 'AD', '41') as int) as point_score_in_game_server_int,
        safe_cast(replace(point_score_in_game_receiver, 'AD', '41') as int) as point_score_in_game_receiver_int,
    
    from tennisabstract_points_parse_scores
),

-- add scores
tennisabstract_points_add_scores as (
    select
        *,
        set_score_in_match_server_int + set_score_in_match_receiver_int + 1 as set_number_in_match,
        game_score_in_set_server_int + game_score_in_set_receiver_int + 1 as game_number_in_set,

    from tennisabstract_points_convert_scores
),

-- get data from shotlog
tennisabstract_point_shotlog as (
    select
        *,

        lower(array_last(point_shotlog)) as last_shot,
        array_length(point_shotlog) as shotlog_length,

    from tennisabstract_points_add_scores
),

-- get point result
tennisabstract_points_result as (
    select
        *,

        -- order matters
        case
            when regexp_contains(last_shot, r'ace') then 'ace'
            when regexp_contains(last_shot, r'double fault') then 'double fault'
            when regexp_contains(last_shot, r'service winner') then 'service winner'
            when regexp_contains(last_shot, r'winner') then 'winner'
            when regexp_contains(last_shot, r'unforced error') then 'unforced error'
            when regexp_contains(last_shot, r'forced error') then 'forced error'
            else null
        end as point_result,

    from tennisabstract_point_shotlog
),

-- get rally length
tennisabstract_points_rally as (
    select
        *,

        -- calculate number of shots based on point result
        case
            when point_result in ('ace', 'double fault', 'forced error', 'service winner', 'unforced error', 'winner') then shotlog_length
            else null
        end as number_of_shots,

        -- calculate rally length based on point result
        case
            when point_result in ('ace', 'service winner', 'winner') then shotlog_length
            when point_result in ('double fault', 'forced error', 'unforced error') then shotlog_length - 1
            else null
        end as rally_length,

    from tennisabstract_points_result
),


-- get running counts for determining <match_unit> # within <match_unit>
tennisabstract_points_running_numbers as (
    select
        *,

        -- game number in match
        dense_rank() over (
            partition by bk_match
            order by set_number_in_match, game_number_in_set
        ) as game_number_in_match,

    from tennisabstract_points_rally
),

-- create bks
tennisabstract_points_bks as (
    select
        *,

        {{ generate_bk_game(
            bk_match_col='bk_match',
            game_number_col='game_number_in_match'
        )}} as bk_game,

        {{ generate_bk_set(
            bk_match_col='bk_match',
            set_number_col='set_number_in_match'
        )}} as bk_set,

    from tennisabstract_points_running_numbers
),

-- get point winner from rally result
tennisabstract_points_point_winner_result as (
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

    from tennisabstract_points_bks
),

-- calculate player scores
tennisabstract_points_player_scores as (
    select
        *,

        -- player one scores
        lpad(
            (
                case
                    when bk_point_server = bk_point_player_one then set_score_in_match_server
                    when bk_point_receiver = bk_point_player_one then set_score_in_match_receiver
                    else null
                end
            ),
            2,
            '0'
        )  as set_score_in_match_player_one,
        case
            when bk_point_server = bk_point_player_one then set_score_in_match_server_int
            when bk_point_receiver = bk_point_player_one then set_score_in_match_receiver_int
            else null
        end as set_score_in_match_player_one_int,
        lpad(
            (
                case
                    when bk_point_server = bk_point_player_one then game_score_in_set_server
                    when bk_point_receiver = bk_point_player_one then game_score_in_set_receiver
                    else null
                end
            ),
            4,
            '0'
        ) as game_score_in_set_player_one,
        case
            when bk_point_server = bk_point_player_one then game_score_in_set_server_int
            when bk_point_receiver = bk_point_player_one then game_score_in_set_receiver_int
            else null
        end as game_score_in_set_player_one_int,
        lpad(
            cast(
                (
                    case
                        when bk_point_server = bk_point_player_one then point_score_in_game_server_int
                        when bk_point_receiver = bk_point_player_one then point_score_in_game_receiver_int
                        else null
                    end 
                )
            as string),
            4,
            '0'
        ) as point_score_in_game_player_one, -- use the 'int' column since 'AD' recasted
        case
            when bk_point_server = bk_point_player_one then point_score_in_game_server_int
            when bk_point_receiver = bk_point_player_one then point_score_in_game_receiver_int
            else null
        end as point_score_in_game_player_one_int,
        
        -- player 2 scores
        lpad(
            (
                case
                    when bk_point_server = bk_point_player_two then set_score_in_match_server
                    when bk_point_receiver = bk_point_player_two then set_score_in_match_receiver
                    else null
                end
            ),
            2,
            '0'
        )  as set_score_in_match_player_two,
        case
            when bk_point_server = bk_point_player_two then set_score_in_match_server_int
            when bk_point_receiver = bk_point_player_two then set_score_in_match_receiver_int
            else null
        end as set_score_in_match_player_two_int,
        lpad(
            (
                case
                    when bk_point_server = bk_point_player_two then game_score_in_set_server
                    when bk_point_receiver = bk_point_player_two then game_score_in_set_receiver
                    else null
                end
            ),
            4,
            '0'
        ) as game_score_in_set_player_two,
        case
            when bk_point_server = bk_point_player_two then game_score_in_set_server_int
            when bk_point_receiver = bk_point_player_two then game_score_in_set_receiver_int
            else null
        end as game_score_in_set_player_two_int,
        lpad(
            cast(
                (
                    case
                        when bk_point_server = bk_point_player_two then point_score_in_game_server_int
                        when bk_point_receiver = bk_point_player_two then point_score_in_game_receiver_int
                        else null
                    end 
                )
            as string),
            4,
            '0'
        ) as point_score_in_game_player_two, -- use the 'int' column since 'AD' recasted
        case
            when bk_point_server = bk_point_player_two then point_score_in_game_server_int
            when bk_point_receiver = bk_point_player_two then point_score_in_game_receiver_int
            else null
        end as point_score_in_game_player_two_int,
        
    from tennisabstract_points_point_winner_result
),

-- concatenate player scores
tennisabstract_points_players_concat as (
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

    from tennisabstract_points_player_scores
),

-- get 'next point' values (including point winner)
tennisabstract_points_next_point as (
    select
        p.*,

        -- calculate point winner based on next point score
        case
            -- scenarios for player one
            when 1=0
                -- when player one's next point score > previous one
                or p_next.point_score_in_match_full_player_one > p.point_score_in_match_full_player_one
                -- when player two's next point score < previous one (could be the case when going from AD to 40)
                or p_next.point_score_in_match_full_player_two < p.point_score_in_match_full_player_two
            then p.bk_point_player_one

             -- scenarios for player two
            when 1=0
                -- when player two's next point score > previous one
                or p_next.point_score_in_match_full_player_two > p.point_score_in_match_full_player_two
                -- when player one's next point score < previous one (could be the case when going from AD to 40)
                or p_next.point_score_in_match_full_player_one < p.point_score_in_match_full_player_one
            then p.bk_point_player_two
            else null
        end as bk_point_winner_next_point,

    from tennisabstract_points_players_concat as p
    left join tennisabstract_points_players_concat as p_next on 1=1
        and p.bk_match = p_next.bk_match
        and p.point_number_in_match + 1 = p_next.point_number_in_match
),

-- calculate point winner
tennisabstract_points_point_winner as (
    select
        *,

        -- calculate point winner based on current winner columns
        -- coalesce(bk_point_winner_result, bk_point_winner_next_point) as bk_point_winner,
        bk_point_winner_result as bk_point_winner,
        
    from tennisabstract_points_next_point
),

-- get point loser
tennisabstract_points_point_loser as (
    select
        *,

        case
            when bk_point_winner = bk_point_server then bk_point_receiver
            when bk_point_winner = bk_point_receiver then bk_point_server
            else null
        end as bk_point_loser,

    from tennisabstract_points_point_winner
),

-- get point number in <match unit>
tennisabstract_points_point_number as (
    select
        *,

        -- point number in set
        row_number() over (
            partition by bk_match, set_number_in_match
            order by game_number_in_set, point_number_in_match
        ) as point_number_in_set,

        -- point number in game
        row_number() over (
            partition by bk_match, set_number_in_match, game_number_in_set
            order by point_number_in_match
        ) as point_number_in_game,

    from tennisabstract_points_point_loser
),

-- get point side
tennisabstract_points_point_side as (
    select
        *,

        case
            -- determine side based on non-tiebreaker scores
            -- TODO: figure out if ad scoring is used when '40-40'
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
        end as point_side,

    from tennisabstract_points_point_number
),

-- create array of distinct, non-null winner values
tennisabstract_points_winners as (
  select
    p.*,
    (
      select array_agg(x)
      from (
        select distinct x
        from unnest([p.bk_point_winner_result, p.bk_point_winner_next_point]) as x
        where x is not null
      )
    ) as bk_point_winner_array
  from tennisabstract_points_point_side as p
),

-- calculate 'quality' flag
tennisabstract_points_quality_flag as (
    select
        *,

        case
            when 1=0
                or bk_point_winner is null
                or array_length(bk_point_winner_array) != 1
                or point_result is null
                or point_result not in ('ace', 'double fault', 'forced error', 'service winner', 'unforced error', 'winner')
            then false
            else true
        end as is_quality_point,

    from tennisabstract_points_winners
),

final as (
    select
        bk_point,
        bk_match,
        match_url,
        point_number_in_match,
        set_score_in_match,
        game_score_in_set,
        point_score_in_game,
        point_shotlog,

        bk_point_player_one,
        bk_point_player_two,
        bk_point_server,
        bk_point_receiver,

        set_score_in_match_server,
        set_score_in_match_receiver,
        game_score_in_set_server,
        game_score_in_set_receiver,
        point_score_in_game_server,
        point_score_in_game_receiver,

        set_score_in_match_server_int,
        set_score_in_match_receiver_int,
        point_score_in_game_server_int,
        point_score_in_game_receiver_int,

        game_number_in_set,

        point_result,
        number_of_shots,
        rally_length,

        game_number_in_match,
        set_number_in_match,

        bk_game,
        bk_set,

        bk_point_winner_result,

        set_score_in_match_player_one,
        set_score_in_match_player_one_int,
        game_score_in_set_player_one,
        game_score_in_set_player_one_int,
        point_score_in_game_player_one,
        point_score_in_game_player_one_int,
        set_score_in_match_player_two,
        set_score_in_match_player_two_int,
        game_score_in_set_player_two,
        game_score_in_set_player_two_int,
        point_score_in_game_player_two,
        point_score_in_game_player_two_int,

        point_score_in_match_full_player_one,
        point_score_in_match_full_player_two,

        bk_point_winner_next_point,

        bk_point_winner,
        
        bk_point_loser,

        point_number_in_set,
        point_number_in_game,

        point_side,

        bk_point_winner_array,

        is_quality_point,
    
    from tennisabstract_points_quality_flag
)

select * from final








-- -- calculate player scores
-- tennisabstract_matches_points_players as (
--     select
--         *,


--         lpad(
--             (
--                 case
--                     when bk_point_server = bk_match_player_one then game_score_in_set_server
--                     when bk_point_receiver = bk_match_player_one then game_score_in_set_receiver
--                     else null
--                 end
--             ),
--             4,
--             '0'
--         ) as game_score_in_set_player_one,
--         case
--             when bk_point_server = bk_match_player_one then game_score_in_set_server_int
--             when bk_point_receiver = bk_match_player_one then game_score_in_set_receiver_int
--             else null
--         end as game_score_in_set_player_one_int,
--         lpad(
--             cast(
--                 (
--                     case
--                         when bk_point_server = bk_match_player_one then point_score_in_game_server_int
--                         when bk_point_receiver = bk_match_player_one then point_score_in_game_receiver_int
--                         else null
--                     end 
--                 )
--             as string),
--             4,
--             '0'
--         ) as point_score_in_game_player_one, -- use the 'int' column since 'AD' recasted
--         case
--             when bk_point_server = bk_match_player_one then point_score_in_game_server_int
--             when bk_point_receiver = bk_match_player_one then point_score_in_game_receiver_int
--             else null
--         end as point_score_in_game_player_one_int,
        

--         lpad(
--             (
--                 case
--                     when bk_point_server = bk_match_player_two then game_score_in_set_server
--                     when bk_point_receiver = bk_match_player_two then game_score_in_set_receiver
--                     else null
--                 end
--             ),
--             4,
--             '0'
--         ) as game_score_in_set_player_two,
--         case
--             when bk_point_server = bk_match_player_two then game_score_in_set_server_int
--             when bk_point_receiver = bk_match_player_two then game_score_in_set_receiver_int
--             else null
--         end as game_score_in_set_player_two_int,
--         lpad(
--             cast(
--                 (
--                     case
--                         when bk_point_server = bk_match_player_two then point_score_in_game_server_int
--                         when bk_point_receiver = bk_match_player_two then point_score_in_game_receiver_int
--                         else null
--                     end 
--                 )
--             as string),
--             4,
--             '0'
--         ) as point_score_in_game_player_two, -- use the 'int' column since 'AD' recasted
--         case
--             when bk_point_server = bk_match_player_two then point_score_in_game_server_int
--             when bk_point_receiver = bk_match_player_two then point_score_in_game_receiver_int
--             else null
--         end as point_score_in_game_player_two_int,
        
--     from tennisabstract_matches_points_point_winner_result
-- ),

-- -- join to tournaments to get formats
-- tennisabstract_matches_points_tournament_formats as (
--     select
--         p.*,

--         t.best_of_sets,
--         t.sets_to_win,
--         t.games_per_set,
--         t.tiebreak_trigger_game,
--         t.tiebreak_points,
--         t.final_set_tiebreak_trigger_game,
--         t.final_set_tiebreak_points,
--         t.is_ad_scoring,

--         -- is set the last set in the match
--         p.set_number_in_match = t.best_of_sets as is_final_set,

--     from tennisabstract_matches_points_players as p
--     left join tournaments as t on p.bk_match_tournament = t.bk_tournament
-- ),

-- -- determine tiebreak flags for games
-- tennisabstract_matches_points_game_tiebreak_flags as (
--     select
--         *,

--         -- determine if game is tiebreak game
--         case
--             -- when in final set
--             when 1=1
--                 and is_final_set = true
--                 and game_score_in_set = final_set_tiebreak_trigger_game
--             then true

--             -- when not in final set
--             when 1=1
--                 and is_final_set = false
--                 and game_score_in_set = tiebreak_trigger_game
--             then true

--             else false
        
--         end as is_tiebreak_game,

--         -- determine if 
--     from tennisabstract_matches_points_tournament_formats
-- ),

-- -- determine set tiebreak flags
-- tennisabstract_matches_points_set_tiebreak_flags as (
--     select
--         *,

--         -- check if any values within set are 'tiebreak games'
--         max(cast(is_tiebreak_game as int)) over (partition by bk_set) = 1 as is_tiebreak_set,

--     from tennisabstract_matches_points_game_tiebreak_flags
-- ),

-- -- determine point flags
-- tennisabstract_matches_points_game_point_flags as (
--     select
--         *,

--         -- determine game point for player one
--         case

--             -- when in tiebreak --> false
--             when is_tiebreak_game = true then false

--             -- when no ad scoring --> true when player is serving at 'deuce'
--             when 1=1
--                 and is_ad_scoring = false
--                 and bk_point_server = bk_match_player_one
--                 and point_score_in_game = '40-40'
--             then true

--             -- when ad scoring --> true when player is serving up '40-{something}' or has advantage
--             when 1=1
--                 and is_ad_scoring = true
--                 and bk_point_server = bk_match_player_one
--                 and point_score_in_game_player_one_int >= 40
--                 and point_score_in_game_player_one_int > point_score_in_game_player_two_int
--             then true

--             else false

--         end as is_game_point_player_one,

--         -- determine game point for player two
--         case

--             -- when in tiebreak --> false
--             when is_tiebreak_game = true then false

--             -- when no ad scoring --> true when player is serving at 'deuce'
--             when 1=1
--                 and is_ad_scoring = false
--                 and bk_point_server = bk_match_player_two
--                 and point_score_in_game = '40-40'
--             then true

--             -- when ad scoring --> true when player is serving up '40-{something}' or has advantage
--             when 1=1
--                 and is_ad_scoring = true
--                 and bk_point_server = bk_match_player_two
--                 and point_score_in_game_player_two_int >= 40
--                 and point_score_in_game_player_two_int > point_score_in_game_player_one_int
--             then true

--             else false

--         end as is_game_point_player_two,

--         -- determine break point for player one
--         case

--             -- when in tiebreak --> false
--             when is_tiebreak_game = true then false

--             -- when no ad scoring --> true when player is receiving at 'deuce'
--             when 1=1
--                 and is_ad_scoring = false
--                 and bk_point_receiver = bk_match_player_one
--                 and point_score_in_game = '40-40'
--             then true

--             -- when ad scoring --> true when player is receiving up '{something}-40' or has advantage
--             when 1=1
--                 and is_ad_scoring = true
--                 and bk_point_receiver = bk_match_player_one
--                 and point_score_in_game_player_one_int >= 40
--                 and point_score_in_game_player_one_int > point_score_in_game_player_two_int
--             then true

--             else false

--         end as is_break_point_player_one,

--         -- determine break point for player two
--         case

--             -- when in tiebreak --> false
--             when is_tiebreak_game = true then false

--             -- when no ad scoring --> true when player is receiving at 'deuce'
--             when 1=1
--                 and is_ad_scoring = false
--                 and bk_point_receiver = bk_match_player_two
--                 and point_score_in_game = '40-40'
--             then true

--             -- when ad scoring --> true when player is receiving up '{something}-40' or has advantage
--             when 1=1
--                 and is_ad_scoring = true
--                 and bk_point_receiver = bk_match_player_two
--                 and point_score_in_game_player_two_int >= 40
--                 and point_score_in_game_player_two_int > point_score_in_game_player_one_int
--             then true

--             else false

--         end as is_break_point_player_two,

--     from tennisabstract_matches_points_set_tiebreak_flags
-- ),

-- -- determine set point flags
-- tennisabstract_matches_points_game_set_flags as (
--     select
--         *,

--         -- determine set point for player one
--         case

--             -- when invalid or undefined format --> skip logic
--             when best_of_sets is null or games_per_set is null then false

--             -- tiebreak scenarios (regular or final sets)
--             when is_tiebreak_game = true then
                
--                 case

--                     -- when final set tiebreak
--                     when 1=1
--                         and is_final_set = true -- in final set
--                         and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
--                         and final_set_tiebreak_points is not null -- format explicitly defines final set TB points
--                         and point_score_in_game_player_one_int >= final_set_tiebreak_points - 1 -- one point away from target
--                         and point_score_in_game_player_one_int > point_score_in_game_player_two_int -- leading
--                     then true

--                     -- when regular (non-final) set tiebreak
--                     when 1=1
--                         and is_final_set = false -- not final set
--                         and tiebreak_trigger_game is not null -- format defines regular set TB
--                         and tiebreak_points is not null -- format defines regular set TB points
--                         and point_score_in_game_player_one_int >= tiebreak_points - 1 -- one point away from target
--                         and point_score_in_game_player_one_int > point_score_in_game_player_two_int -- leading
--                     then true

--                     -- when 'extended' tiebreak (no limit or missing config)
--                     when 1=1
--                         and (tiebreak_points is null or final_set_tiebreak_points is null) -- undefined upper limit
--                         and point_score_in_game_player_one_int - point_score_in_game_player_two_int = 1 -- leading by 1
--                     then true

--                     else false

--                 end

--             -- non-tiebreak scenarios
--             when is_tiebreak_game = false then

--                 case

--                     -- normal (non-final) sets
--                     when 1=1
--                         and is_final_set = false -- not last set
--                         and game_score_in_set_player_one_int = games_per_set - 1 -- one game away
--                         and game_score_in_set_player_one_int > game_score_in_set_player_two_int -- leading
--                         and (is_game_point_player_one = true or is_break_point_player_one = true) -- point could close out game
--                     then true

--                     -- when final set with standard or super tiebreak
--                     when 1=1
--                         and is_final_set = true -- in final set
--                         and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
--                         and game_score_in_set_player_one_int = games_per_set - 1 -- one game away
--                         and game_score_in_set_player_one_int > game_score_in_set_player_two_int -- leading
--                         and (is_game_point_player_one = true or is_break_point_player_one = true) -- point could close out game
--                     then true

--                     -- when advantage final set (no tiebreak)
--                     when 1=1
--                         and is_final_set = true -- in final set
--                         and (final_set_tiebreak_trigger_game is null or final_set_tiebreak_points is null) -- no TB defined
--                         and (game_score_in_set_player_one_int - game_score_in_set_player_two_int = 1) -- leading by 1 game
--                         and (is_game_point_player_one = true or is_break_point_player_one = true) -- point could close out game
--                     then true 

--                     else false

--                 end

--             else false

--         end as is_set_point_player_one,

--         -- determine set point for player two
--         case

--             -- when invalid or undefined format --> skip logic
--             when best_of_sets is null or games_per_set is null then false

--             -- tiebreak scenarios (regular or final sets)
--             when is_tiebreak_game = true then
                
--                 case

--                     -- when final set tiebreak
--                     when 1=1
--                         and is_final_set = true -- in final set
--                         and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
--                         and final_set_tiebreak_points is not null -- format explicitly defines final set TB points
--                         and point_score_in_game_player_two_int >= final_set_tiebreak_points - 1 -- one point away from target
--                         and point_score_in_game_player_two_int > point_score_in_game_player_one_int -- leading
--                     then true

--                     -- when regular (non-final) set tiebreak
--                     when 1=1
--                         and is_final_set = false -- not final set
--                         and tiebreak_trigger_game is not null -- format defines regular set TB
--                         and tiebreak_points is not null -- format defines regular set TB points
--                         and point_score_in_game_player_two_int >= tiebreak_points - 1 -- one point away from target
--                         and point_score_in_game_player_two_int > point_score_in_game_player_one_int -- leading
--                     then true

--                     -- when 'extended' tiebreak (no limit or missing config)
--                     when 1=1
--                         and (tiebreak_points is null or final_set_tiebreak_points is null) -- undefined upper limit
--                         and point_score_in_game_player_two_int - point_score_in_game_player_one_int = 1 -- leading by 1
--                     then true

--                     else false

--                 end

--             -- non-tiebreak scenarios
--             when is_tiebreak_game = false then

--                 case

--                     -- normal (non-final) sets
--                     when 1=1
--                         and is_final_set = false -- not last set
--                         and game_score_in_set_player_two_int = games_per_set - 1 -- one game away
--                         and game_score_in_set_player_two_int > game_score_in_set_player_one_int -- leading
--                         and (is_game_point_player_two = true or is_break_point_player_two = true) -- point could close out game
--                     then true

--                     -- when final set with standard or super tiebreak
--                     when 1=1
--                         and is_final_set = true -- in final set
--                         and final_set_tiebreak_trigger_game is not null -- format explicitly defines final set TB
--                         and game_score_in_set_player_two_int = games_per_set - 1 -- one game away
--                         and game_score_in_set_player_two_int > game_score_in_set_player_one_int -- leading
--                         and (is_game_point_player_two = true or is_break_point_player_two = true) -- point could close out game
--                     then true

--                     -- when advantage final set (no tiebreak)
--                     when 1=1
--                         and is_final_set = true -- in final set
--                         and (final_set_tiebreak_trigger_game is null or final_set_tiebreak_points is null) -- no TB defined
--                         and (game_score_in_set_player_two_int - game_score_in_set_player_one_int = 1) -- leading by 1 game
--                         and (is_game_point_player_two = true or is_break_point_player_two = true) -- point could close out game
--                     then true 

--                     else false

--                 end

--             else false

--         end as is_set_point_player_two,

--         -- get cumulative sets won by player one
--         max(set_score_in_match_player_one_int) over (
--             partition by bk_match
--             order by point_number_in_match
--             rows between unbounded preceding and current row
--         ) as cumulative_set_score_in_match_player_one_int,

--         -- get cumulative sets won by player two
--         max(set_score_in_match_player_two_int) over (
--             partition by bk_match
--             order by point_number_in_match
--             rows between unbounded preceding and current row
--         ) as cumulative_set_score_in_match_player_two_int,

--     from tennisabstract_matches_points_game_point_flags
-- ),

-- -- determine match point flags
-- tennisabstract_matches_points_game_match_flags as (
--     select
--         *,

--         -- determine match point for player one
--         case

--             -- when invalid or undefined format --> skip
--             when (best_of_sets is null or sets_to_win is null) then false

--             -- when player has reached required sets to win --> skip
--             when cumulative_set_score_in_match_player_one_int >= sets_to_win then false

--             -- when player is one set away and has set point
--             when 1=1
--                 and cumulative_set_score_in_match_player_one_int = sets_to_win - 1 -- 1 set away
--                 and is_set_point_player_one -- set point
--             then true

--             else false

--         end as is_match_point_player_one,

--         -- determine match point for player two
--         case

--             -- when invalid or undefined format --> skip
--             when (best_of_sets is null or sets_to_win is null) then false

--             -- when player has reached required sets to win --> skip
--             when cumulative_set_score_in_match_player_two_int >= sets_to_win then false

--             -- when player is one set away and has set point
--             when 1=1
--                 and cumulative_set_score_in_match_player_two_int = sets_to_win - 1  -- 1 set away
--                 and is_set_point_player_two                                         -- set point
--             then true

--             else false

--         end as is_match_point_player_two,

--     from tennisabstract_matches_points_game_set_flags
-- ),

-- -- concatenate player scores
-- tennisabstract_matches_points_players_concat as (
--     select
--         *,

--         -- player one
--         concat(
--             set_score_in_match_player_one,
--             '-',
--             game_score_in_set_player_one,
--             '-',
--             point_score_in_game_player_one
--         ) as point_score_in_match_full_player_one,

--         -- player two
--         concat(
--             set_score_in_match_player_two,
--             '-',
--             game_score_in_set_player_two,
--             '-',
--             point_score_in_game_player_two
--         ) as point_score_in_match_full_player_two,

--     from tennisabstract_matches_points_game_match_flags
-- ),

-- -- get 'next point' values (including point winner)
-- tennisabstract_matches_points_next_point as (
--     select
--         p.*,

--         -- calculate point winner based on next point score
--         case
--             when p_next.point_score_in_match_full_player_one > p.point_score_in_match_full_player_one then p.bk_match_player_one
--             when p_next.point_score_in_match_full_player_two > p.point_score_in_match_full_player_two then p.bk_match_player_two
--             else null
--         end as bk_point_winner_next_point,

--     from tennisabstract_matches_points_players_concat as p
--     left join tennisabstract_matches_points_players_concat as p_next on 1=1
--         and p.bk_match = p_next.bk_match
--         and p.point_number_in_match + 1 = p_next.point_number_in_match
-- ),

-- -- calculate point winner
-- tennisabstract_matches_points_point_winner as (
--     select
--         *,

--         -- calculate point winner based on current winner columns
--         coalesce(bk_point_winner_result, bk_point_winner_next_point) as bk_point_winner,
    
--     from tennisabstract_matches_points_next_point
-- ),


-- -- get point loser
-- tennisabstract_matches_points_point_loser as (
--     select
--         *,

--         case
--             when bk_point_winner = bk_point_server then bk_point_receiver
--             when bk_point_winner = bk_point_receiver then bk_point_server
--             else null
--         end as bk_point_loser,

--     from tennisabstract_matches_points_point_winner
-- ),

-- -- get last point in match units (game, sets)
-- tennisabstract_matches_points_last_point as (
--     select
--         *,

--         point_number_in_match = max(point_number_in_match) over (partition by bk_match, bk_set, bk_game) as is_last_point_in_game,
--         point_number_in_match = max(point_number_in_match) over (partition by bk_match, bk_set) as is_last_point_in_set,

--     from tennisabstract_matches_points_point_loser
-- ),

-- -- flag row data quality
-- tennisabstract_matches_points_quality as (
--     select
--         *,

--         -- flag rows
--         case
--             when 1=1
--                 and bk_point_winner_result = bk_point_winner_next_point
--                 and point_result is not null
--             then true
--             else false
--         end as is_quality_point,

--     from tennisabstract_matches_points_last_point
-- ),
