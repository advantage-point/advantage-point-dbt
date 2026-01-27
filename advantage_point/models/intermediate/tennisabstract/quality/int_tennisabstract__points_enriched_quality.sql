with

tennisabstract_points as (
    select * from {{ ref('int_tennisabstract__points_enriched') }}
    where is_quality_point = false
)

select
    match_url,
    point_number_in_match,
    set_score_in_match,
    game_score_in_set,
    point_score_in_game,
    array_to_string(point_shotlog, ';\n') as point_shotlog_string,
    point_result,
    array_to_string(bk_point_winner_array, ';\n') as bk_point_winner_array_string,
    bk_point_winner,
from tennisabstract_points