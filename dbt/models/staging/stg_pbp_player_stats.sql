with source as (

    select * from {{ source('raw', 'pbp') }}

),

cleaned as (

    select
        season,
        week,
        game_id,
        posteam as team,

        -- passing
        passer_player_id,
        passer_player_name,
        pass_attempt,
        complete_pass,
        passing_yards,
        pass_touchdown,
        interception,

        -- rushing
        rusher_player_id,
        rusher_player_name,
        rush_attempt,
        rushing_yards,
        rush_touchdown,

        -- receiving
        receiver_player_id,
        receiver_player_name,
        receiving_yards,
        air_yards,
        yards_after_catch,

        -- two-point conversions
        two_point_attempt,
        two_point_conv_result,

        -- fumbles
        fumbled_1_player_id,
        fumble,
        fumble_lost,
        fumble_recovery_1_player_id,
        fumble_recovery_1_team,

        -- kicking
        kicker_player_id,
        kicker_player_name,
        field_goal_attempt,
        field_goal_result,
        kick_distance,
        extra_point_attempt,
        extra_point_result,

        -- touchdown = something scored on this play (could belong to
        -- the original ball carrier, not the fumble recoverer).
        -- return_touchdown = the score came from the return itself.
        -- Use return_touchdown, not touchdown, when crediting a
        -- fumble recovery TD -- see fct_player_game_stats for why.
        touchdown,
        return_touchdown

    from source
    where play_type is not null

)

select * from cleaned