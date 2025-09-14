with

tennisabstract_matches as (
    select * from {{ ref('stg__web__tennisabstract__matches') }}
    where audit_column__active_flag = true
),

-- parse out points into own rows
tennisabstract_matches_points as (
    select
        *,
        (
            select array_agg(player order by player)
            from unnest(array[match_player_one, match_player_two]) as player
        ) as match_players,
        cast(json_value(point_dict, '$.point_number') as integer) as point_number_in_match,
        json_value(point_dict, '$.server') as point_server,
        json_value(point_dict, '$.sets') as set_score_in_match,
        json_value(point_dict, '$.games') as game_score_in_set,
        json_value(point_dict, '$.points') as point_score_in_game,
    from tennisabstract_matches,
    unnest(tennisabstract_matches.match_pointlog) as point_dict
),

-- parse out scores
tennisabstract_matches_points_parse_scores as (
    select
        *,
        cast(split_part(set_score_in_match, '-', 1) as int) as set_score_in_match_server,
        cast(split_part(set_score_in_match, '-', 2) as int) as set_score_in_match_receiver,
        cast(split_part(game_score_in_set, '-', 1) as int) as game_score_in_set_server,
        cast(split_part(game_score_in_set, '-', 2) as int) as game_score_in_set_receiver,
        split_part(point_score_in_game, '-', 1) as point_score_in_game_server,
        split_part(point_score_in_game, '-', 2) as point_score_in_game_receiver,
    from tennisabstract_matches_points
),

-- add scores
tennisabstract_matches_points_add_scores as (
    select
        *,
        set_score_in_match_server + set_score_in_match_receiver + 1 as set_number_in_match,
        game_score_in_set_server + game_score_in_set_receiver + 1 as game_number_in_set,

        -- convert 'AD' to numeric
        cast(replace(point_score_in_game_server, 'AD', '41') as int) as point_score_in_game_server_int,
        cast(replace(point_score_in_game_receiver, 'AD', '41') as int) as point_score_in_game_receiver_int,
    from tennisabstract_matches_points_parse_scores
),

-- add bk_match
tennisabstract_matches_points_bk_match as (
    select
        *,
        {{ generate_bk_match(
            match_date_col='match_date',
            match_gender_col='match_gender',
            match_tournament_col='match_tournament',
            match_round_col='match_round',
            match_players_col='match_players'
        ) }} as bk_match,
    from tennisabstract_matches_points_add_scores
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
    from tennisabstract_matches_points_bk_match
)


select * from tennisabstract_matches_points_running_numbers