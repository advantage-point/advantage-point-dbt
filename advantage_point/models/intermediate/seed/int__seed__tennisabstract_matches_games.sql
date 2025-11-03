with

matches_games as (
    select * from {{ ref('stg__seed__tennisabstract_matches_games') }}
),

-- split match url (everything after 'https://www.tennisabstract.com/charting/')
matches_games_url_split as (
    select
        *,
        split(split(match_url, 'https://www.tennisabstract.com/charting/')[1], '-') as match_url_split,
        
        split(set_score_in_match, '-')[0] as set_score_in_match_server,
        split(set_score_in_match, '-')[1] as set_score_in_match_receiver,
        
        split(game_score_in_set, '-')[0] as game_score_in_set_server,
        split(game_score_in_set, '-')[1] as game_score_in_set_receiver,

    from matches_games
),

-- parse out match info: {date}-{gender}-{tournament}-{round}-{player1}-{player2}.html
matches_games_match_info as (
    select
        *,

        parse_date('%Y%m%d',match_url_split[0]) as match_date,
        match_url_split[1] as match_gender,
        replace(match_url_split[2], '_', ' ') as match_tournament,
        match_url_split[3] as match_round,
        replace(match_url_split[4], '_', ' ') as match_player_one,
        replace(replace(match_url_split[5], '_', ' '), '.html', '') as match_player_two,

        safe_cast(set_score_in_match_server as int) as set_score_in_match_server_int,
        safe_cast(set_score_in_match_receiver as int) as set_score_in_match_receiver_int,
        
        safe_cast(game_score_in_set_server as int) as game_score_in_set_server_int,
        safe_cast(game_score_in_set_receiver as int) as game_score_in_set_receiver_int,

    from matches_games_url_split
),

-- get game receiver
matches_games_receiver as (
    select
        *,

        -- get receiver
        -- compare lower case in case weird capitalization in names like 'McHale'
        case
            when lower(game_server) = lower(match_player_one) then match_player_two
            when lower(game_server) = lower(match_player_two) then match_player_one
            else null
        end as game_receiver,

    from matches_games_match_info
),

-- add scores
matches_games_add_scores as (
    select
        *,

        extract(year from match_date) as match_year,


        set_score_in_match_server_int + set_score_in_match_receiver_int + 1 as set_number_in_match,
        game_score_in_set_server_int + game_score_in_set_receiver_int + 1 as game_number_in_set,

    from matches_games_receiver
),

-- create bks
matches_games_bk as (
    select
        *,

        -- date
        {{ generate_bk_date(
            date_col='match_date'
        ) }} as bk_match_date,
        
        -- tournament
        {{ generate_bk_tournament(
            tournament_year_col='match_year',
            tournament_event_col='match_event',
            tournament_name_col='match_tournament'
        )}} as bk_match_tournament,

        -- players
        {{ generate_bk_player(
            player_name_col='match_player_one',
            player_gender_col='match_gender'
        ) }} as bk_match_player_one,
        {{ generate_bk_player(
            player_name_col='match_player_two',
            player_gender_col='match_gender'
        ) }} as bk_match_player_two,
        {{ generate_bk_player(
            player_name_col='game_server',
            player_gender_col='match_gender'
        ) }} as bk_game_server,
        {{ generate_bk_player(
            player_name_col='game_receiver',
            player_gender_col='match_gender'
        ) }} as bk_game_receiver,
    from matches_games_add_scores
),

-- create player array string (for use in unique id)
matches_games_match_players as (
    select
        *,
        (
            select array_agg(player order by player)
            from unnest(array[match_player_one, match_player_two]) as player
        ) as match_players,
        (
            select array_agg(player order by player)
            from unnest(array[bk_match_player_one, bk_match_player_two]) as player
        ) as bk_match_players
    from matches_games_bk
),

-- create match bk
matches_games_bk_match as (
    select
        *,
        {{ generate_bk_match(
            bk_match_date_col='bk_match_date',
            bk_match_tournament_col='bk_match_tournament',
            match_round_col='match_round',
            bk_match_players_col='bk_match_players'
        ) }} as bk_match,
    from matches_games_match_players
),

-- get running counts for determining <match_unit> # within <match_unit>
matches_games_running_numbers as (
    select
        *,
        dense_rank() over (
            partition by bk_match
            order by set_number_in_match, game_number_in_set
        ) as game_number_in_match,
        
    from matches_games_bk_match
),

-- create game and set bks
matches_games_child_bks as (
    select
        *,
        {{ generate_bk_set(
            bk_match_col='bk_match',
            set_number_col='set_number_in_match'
        )}} as bk_set,
        {{ generate_bk_game(
            bk_match_col='bk_match',
            game_number_col='game_number_in_match'
        )}} as bk_game,
    from matches_games_running_numbers
),

-- create tiebreaker flag
matches_games_tiebreaker as (
    select
        *,

        case
            when game_score_in_set in ('6-6') then true
            when game_score_in_set not in ('6-6') then false
            else null
        end as is_tiebreaker,
    
    from matches_games_child_bks
),

final as (
    select
        bk_game,
        bk_match,
        game_number_in_match,

        match_url,

        bk_set,
        game_number_in_set,

        bk_game_server,
        bk_game_receiver,

        game_score_in_set,
        game_score_in_set_server,
        game_score_in_set_receiver,
        game_score_in_set_server_int,
        game_score_in_set_receiver_int,

        is_tiebreaker,
  from matches_games_tiebreaker
)

select * from final