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
        json_value(point_dict, '$.server') as point_server,
        
        regexp_replace(
            json_value(point_dict, '$.sets'),
            r'[‐-‒–—−]',
            '-'
        ) as set_score_in_match,
        
        regexp_replace(
            json_value(point_dict, '$.games'),
            r'[‐-‒–—−]',
            '-'
        ) as game_score_in_set,

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
    
    from tennisabstract_matches_points
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

        -- result is is in last element of shot log: {shot},{optional space}{point result}{optional period}
        regexp_extract(
            array_last(point_shotlog),
            r',\s*([^.]+)\.?'
        ) as point_result,

        -- get number of shots
        array_length(point_shotlog) as number_of_shots,

    from tennisabstract_matches_points_point_side
),

-- get rally length
tennisabstract_matches_points_rally as (
    select
        *,

        -- calculate rally length based on point result
        case
            when point_result in ('ace', 'service winner', 'winner') then number_of_shots
            when point_result in ('double fault', 'forced error', 'unforced error') then number_of_shots - 1
            else null
        end as rally_length,

    from tennisabstract_matches_points_point_result
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

    from tennisabstract_matches_points_rally
),

final as (
    select
        {{ generate_bk_point(
            bk_match_col='bk_match',
            point_number_col='point_number_in_match'
        )}} as bk_point,
        bk_match,
        point_number_in_match,
        point_dict,
        point_server,

        -- get receiver
        case
                when point_server = match_player_one then match_player_two
                when point_server = match_player_two then match_player_one
                else null
            end as point_receiver,

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
    from tennisabstract_matches_points_game_point_type
)

select * from final