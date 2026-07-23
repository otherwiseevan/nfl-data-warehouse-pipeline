with player_week as (

    select * from {{ ref('fct_player_week') }}

),

sleeper as (

    select * from {{ ref('stg_sleeper_rosters') }}

),

joined as (

    select
        player_week.season,
        player_week.week,
        player_week.team,
        player_week.player_id,
        player_week.sleeper_id,
        player_week.player_name,
        player_week.position,
        player_week.status,
        sleeper.roster_id,
        sleeper.owner_name,
        sleeper.wins,
        sleeper.losses
    from player_week
    left join sleeper
        on player_week.sleeper_id = sleeper.sleeper_id

)

select * from joined