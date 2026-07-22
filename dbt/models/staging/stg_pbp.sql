with source as (

    select * from {{ source('raw', 'pbp') }}

),

cleaned as (

    select
        season,
        game_id,
        week,
        posteam as possession_team,
        defteam as defense_team,
        play_type,
        yards_gained,
        epa
    from source
    where play_type is not null
      and posteam is not null  -- excludes no_play rows (timeouts, presnap penalties, etc.) with no possessing team

)

select * from cleaned
