with source as (

    select * from {{ source('raw', 'rosters') }}

),

cleaned as (

    select
        season,
        week,
        team,
        player_id,
        sleeper_id,
        player_name,
        position,
        years_exp,
        draft_number,
        age,
        status
    from source

)

select * from cleaned
