{{
    config(
        materialized='table'
    )
}}

with

tennisabstract_matches_points as (
    select * from {{ ref('int__web__tennisabstract__matches__points') }}
),

-- split out shotlog into rows
tennisabstract_matches_points_shots as (
    select
        * except(ordinality, shot_text),
        
        -- add 1 so first shot starts at 1
        ordinality + 1 as shot_number,

        trim(shot_text) as shot_text,

    from tennisabstract_matches_points,
    unnest(point_shotlog) as shot_text with offset as ordinality
),

-- split serves into own rows
tennisabstract_matches_points_shots_serves as (
  select
    * except(serve_text) replace (
      trim(serve_text) as shot_text
    ),

    -- sort 1st and 2nd serves
    case
      when serve_text like '%1st%' then 1
      when serve_text like '%2nd%' then 2
      else null
    end as serve_sort,

  from tennisabstract_matches_points_shots,
  unnest(split(shot_text, '.')) as serve_text
  where 1=1
      and shot_number = 1 -- filter for 'serve' rows
      and trim(serve_text) != '' -- filter out blank rows since some rows end with '.'
),

-- filter out non-serves (will be unioned back later)
tennisabstract_matches_points_shots_non_serves as (
  select
    *,
    -- assign a sort value so that rally shots are ordered after serve shots
    3 as serve_sort,
  from tennisabstract_matches_points_shots
  where shot_number != 1
),

-- union serve rows with non-serve rows
tennisabstract_matches_points_shots_union as (
  (select * from tennisabstract_matches_points_shots_serves)
  union all
  (select * from tennisabstract_matches_points_shots_non_serves)
),

-- get shot number with serve factored
tennisabstract_matches_points_shot_num as (
  select
    *,
    row_number() over (partition by bk_point order by shot_number, serve_sort) as shot_number_in_point,
  from tennisabstract_matches_points_shots_union
),

-- get shot attributes
tennisabstract_matches_points_shot_attributes as (
  select
    *,

    -- check for direction strings
    regexp_extract(
      shot_text,
      r'(?i)crosscourt|down the line|down the middle|down the t|inside-in|inside-out|to body|wide'
    ) as shot_direction,

  -- get shot result
  case
    -- if double fault (in case logic codes it as 'fault' instead)
    when contains_substr(shot_text, 'double fault') then 'double fault'
    else regexp_extract(
      lower(shot_text),
      r'ace|double fault|fault|forced error|service winner|unforced error|winner'
    )
  end as shot_result,

  from tennisabstract_matches_points_shot_num
),

-- get shot type
tennisabstract_matches_points_shot_type as (
  select
    *,

    case
      -- if shot was point penalty then NULL
      when contains_substr(shot_text, 'point penalty') then null
      -- if shot was unknown then NULL
      when contains_substr(shot_text, 'unknown') then null
      -- if shot was a 'challenge' then NULL
      when contains_substr(shot_text, 'challenge was incorrect') then null
      -- if shot text is (...) then null
      when left(shot_text, 1) = '(' then null
      -- get text before shot_direction
      when shot_direction is not null then trim(split(shot_text, shot_direction)[0])
      -- if not shot_direction, get text before ','
      when shot_direction is null then trim(split(shot_text, ',')[0])
      else null      
    end as shot_type

  from tennisabstract_matches_points_shot_attributes
),

final as (
  select
    {{generate_bk_shot(
      bk_point_col='bk_point',
      shot_number_col='shot_number_in_point'
    )}} as bk_shot,
    bk_point,
    shot_number_in_point,

    bk_match,
    point_number_in_match,
    match_url,

    point_dict,
    bk_game,
    bk_set,
    point_server,
    point_receiver,
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
    shot_number,
    shot_text,
    serve_sort,
    shot_direction,
    shot_result,
    shot_type,
  
  from tennisabstract_matches_points_shot_type
)

select * from final