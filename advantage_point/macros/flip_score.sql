{% macro flip_score(score_col) %}
    -- Flips any score_col string so that sides of each dash are reversed.
    -- Works for match, set, game, or point-level score_cols (space-delimited).
    -- Example: '6-4 6(5)-7 7-5' → '4-6 7-6(5) 5-7'

    (
        select array_to_string(
            array(
                select
                    -- Swap sides of the dash
                    concat(
                        regexp_extract(segment, r'-(.+)$'),  -- everything after the first dash
                        '-',
                        regexp_extract(segment, r'^(.+?)-') -- everything before the first dash
                    )
                from unnest(split({{ score_col }}, ' ')) as segment
            ),
            ' '
        )
    )

{% endmacro %}
