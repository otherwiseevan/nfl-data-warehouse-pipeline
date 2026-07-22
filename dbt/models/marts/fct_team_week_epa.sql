with pbp as (

    select * from {{ ref('stg_pbp') }}

),

aggregated as (

    select
        season,
        possession_team as team,
        week,
        count(*) as plays,
        avg(epa) as avg_epa
    from pbp
    group by season, possession_team, week

)

select * from aggregated