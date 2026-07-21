with source as (

    select * from {{ source('raw', 'pbp') }}

),

cleaned as (

    select
        game_id,
        week,
        posteam as possession_team,
        defteam as defense_team,
        play_type,
        yards_gained,
        epa
    from source
    where play_type is not null

)

select * from cleaned
