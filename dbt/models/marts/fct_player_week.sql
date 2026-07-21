with rosters as (

    select * from {{ ref('stg_rosters') }}

)

select
  season,
        week,
        team,
        player_id,
        sleeper_id,
        player_name,
        position,
        status,
        years_exp,
        draft_number,
        age
from rosters
