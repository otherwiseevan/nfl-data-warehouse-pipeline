with source as (

    select * from {{ source('raw', 'sleeper_rosters') }}

),

cleaned as (

    select
        roster_id,
        owner_id,
        owner_name,
        is_my_roster,
        sleeper_id,
        wins,
        losses
    from source

)

select * from cleaned