with

int_sets as (
    select * from {{ ref('int__sets') }}
),

dim_match as (
    select * from {{ ref('dim__match') }}
),

-- create sks
set_sks as (
    select
        *,
        {{ generate_sk_set(
            bk_set_col='bk_set'
        ) }} as sk_set,
    from int_sets
),

final as (
    select
        s.sk_set,
        s.bk_set,
        m.sk_match as sk_set_match,
        s.bk_match as bk_set_match,
        s.set_number_in_match,
    from set_sks as s
    left join dim_match as m on s.bk_match = m.bk_match
)

select * from final