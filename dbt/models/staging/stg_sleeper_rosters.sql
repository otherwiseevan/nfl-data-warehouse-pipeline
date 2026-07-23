with source as (

    select * from {{ source('raw', 'sleeper_rosters') }}

),

cleaned as (

    select
        roster_id,
        owner_id,
        owner_name,
        sleeper_id,
        wins,
        losses
    from source

)

select * from cleaned