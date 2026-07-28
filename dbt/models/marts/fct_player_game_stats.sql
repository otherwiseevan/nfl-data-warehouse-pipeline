with passing as (

    select
        season, week, team,
        passer_player_id as player_id,
        passer_player_name as player_name,
        count(*) filter (where pass_attempt = 1) as pass_attempts,
        count(*) filter (where complete_pass = 1) as completions,
        sum(passing_yards) as passing_yards,
        count(*) filter (where pass_touchdown = 1) as passing_tds,
        count(*) filter (where interception = 1) as interceptions,
        count(*) filter (where two_point_attempt = 1 and two_point_conv_result = 'success') as passing_two_pt
    from {{ ref('stg_pbp_player_stats') }}
    where passer_player_id is not null
    group by season, week, team, passer_player_id, passer_player_name

),

rushing as (

    select
        season, week, team,
        rusher_player_id as player_id,
        rusher_player_name as player_name,
        count(*) filter (where rush_attempt = 1) as rush_attempts,
        sum(rushing_yards) as rushing_yards,
        count(*) filter (where rush_touchdown = 1) as rushing_tds,
        count(*) filter (where two_point_attempt = 1 and two_point_conv_result = 'success') as rushing_two_pt
    from {{ ref('stg_pbp_player_stats') }}
    where rusher_player_id is not null
    group by season, week, team, rusher_player_id, rusher_player_name

),

receiving as (

    select
        season, week, team,
        receiver_player_id as player_id,
        receiver_player_name as player_name,
        count(*) filter (where pass_attempt = 1) as targets,
        count(*) filter (where complete_pass = 1) as receptions,
        sum(receiving_yards) as receiving_yards,
        count(*) filter (where pass_touchdown = 1 and complete_pass = 1) as receiving_tds,
        sum(air_yards) as air_yards,
        sum(yards_after_catch) as yards_after_catch,
        count(*) filter (where two_point_attempt = 1 and two_point_conv_result = 'success') as receiving_two_pt
    from {{ ref('stg_pbp_player_stats') }}
    where receiver_player_id is not null
    group by season, week, team, receiver_player_id, receiver_player_name

),

fumbles as (

    select
        season, week,
        fumbled_1_player_id as player_id,
        count(*) filter (where fumble = 1) as fumbles,
        count(*) filter (where fumble_lost = 1) as fumbles_lost
    from {{ ref('stg_pbp_player_stats') }}
    where fumbled_1_player_id is not null
    group by season, week, fumbled_1_player_id

),

-- return_touchdown, not touchdown -- touchdown fires for ANY score on
-- the play, including one where the original ball carrier fought into
-- the end zone after fumbling and a teammate incidentally recovered
-- it. return_touchdown only fires when the recovering player actually
-- ran the ball in themselves. Confirmed against real play descriptions
-- before building this -- touchdown alone over-credits fumble
-- recoveries that weren't actually scored by the recoverer.
fumble_recovery_tds as (

    select
        season, week,
        fumble_recovery_1_player_id as player_id,
        count(*) filter (where return_touchdown = 1) as fumble_recovery_tds
    from {{ ref('stg_pbp_player_stats') }}
    where fumble_recovery_1_player_id is not null
    group by season, week, fumble_recovery_1_player_id

),

kicking as (

    select
        season, week, team,
        kicker_player_id as player_id,
        kicker_player_name as player_name,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result = 'made' and kick_distance < 20) as fg_made_0_19,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result = 'made' and kick_distance between 20 and 29) as fg_made_20_29,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result = 'made' and kick_distance between 30 and 39) as fg_made_30_39,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result = 'made' and kick_distance between 40 and 49) as fg_made_40_49,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result = 'made' and kick_distance between 50 and 59) as fg_made_50_59,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result = 'made' and kick_distance >= 60) as fg_made_60_plus,
        count(*) filter (where field_goal_attempt = 1 and field_goal_result != 'made') as fg_missed,
        count(*) filter (where extra_point_attempt = 1 and extra_point_result = 'good') as pat_made,
        count(*) filter (where extra_point_attempt = 1 and extra_point_result != 'good') as pat_missed
    from {{ ref('stg_pbp_player_stats') }}
    where kicker_player_id is not null
    group by season, week, team, kicker_player_id, kicker_player_name

),

all_player_weeks as (
    select season, week, team, player_id, player_name from passing
    union
    select season, week, team, player_id, player_name from rushing
    union
    select season, week, team, player_id, player_name from receiving
    union
    select season, week, team, player_id, player_name from kicking
),

combined as (

    select
        pw.season, pw.week, pw.team, pw.player_id, pw.player_name,

        coalesce(passing.pass_attempts, 0) as pass_attempts,
        coalesce(passing.completions, 0) as completions,
        coalesce(passing.passing_yards, 0) as passing_yards,
        coalesce(passing.passing_tds, 0) as passing_tds,
        coalesce(passing.interceptions, 0) as interceptions,
        coalesce(passing.passing_two_pt, 0) as passing_two_pt,

        coalesce(rushing.rush_attempts, 0) as rush_attempts,
        coalesce(rushing.rushing_yards, 0) as rushing_yards,
        coalesce(rushing.rushing_tds, 0) as rushing_tds,
        coalesce(rushing.rushing_two_pt, 0) as rushing_two_pt,

        coalesce(receiving.targets, 0) as targets,
        coalesce(receiving.receptions, 0) as receptions,
        coalesce(receiving.receiving_yards, 0) as receiving_yards,
        coalesce(receiving.receiving_tds, 0) as receiving_tds,
        coalesce(receiving.air_yards, 0) as air_yards,
        coalesce(receiving.yards_after_catch, 0) as yards_after_catch,
        coalesce(receiving.receiving_two_pt, 0) as receiving_two_pt,

        coalesce(fumbles.fumbles, 0) as fumbles,
        coalesce(fumbles.fumbles_lost, 0) as fumbles_lost,
        coalesce(fumble_recovery_tds.fumble_recovery_tds, 0) as fumble_recovery_tds,

        coalesce(kicking.fg_made_0_19, 0) as fg_made_0_19,
        coalesce(kicking.fg_made_20_29, 0) as fg_made_20_29,
        coalesce(kicking.fg_made_30_39, 0) as fg_made_30_39,
        coalesce(kicking.fg_made_40_49, 0) as fg_made_40_49,
        coalesce(kicking.fg_made_50_59, 0) as fg_made_50_59,
        coalesce(kicking.fg_made_60_plus, 0) as fg_made_60_plus,
        coalesce(kicking.fg_missed, 0) as fg_missed,
        coalesce(kicking.pat_made, 0) as pat_made,
        coalesce(kicking.pat_missed, 0) as pat_missed

    from all_player_weeks pw
    left join passing on pw.player_id = passing.player_id and pw.season = passing.season and pw.week = passing.week
    left join rushing on pw.player_id = rushing.player_id and pw.season = rushing.season and pw.week = rushing.week
    left join receiving on pw.player_id = receiving.player_id and pw.season = receiving.season and pw.week = receiving.week
    left join fumbles on pw.player_id = fumbles.player_id and pw.season = fumbles.season and pw.week = fumbles.week
    left join fumble_recovery_tds on pw.player_id = fumble_recovery_tds.player_id and pw.season = fumble_recovery_tds.season and pw.week = fumble_recovery_tds.week
    left join kicking on pw.player_id = kicking.player_id and pw.season = kicking.season and pw.week = kicking.week

),

with_fantasy_points as (

    select
        *,
        -- League scoring: half-PPR, Superflex.
        round(
            (passing_yards * 0.04)
            + (passing_tds * 4)
            + (interceptions * -1)
            + (passing_two_pt * 2)
            + (rushing_yards * 0.1)
            + (rushing_tds * 6)
            + (rushing_two_pt * 2)
            + (receiving_yards * 0.1)
            + (receptions * 0.5)
            + (receiving_tds * 6)
            + (receiving_two_pt * 2)
            + (fumbles * -1)
            + (fumbles_lost * -1)
            + (fumble_recovery_tds * 6)
            + (fg_made_0_19 * 3)
            + (fg_made_20_29 * 3)
            + (fg_made_30_39 * 3)
            + (fg_made_40_49 * 4)
            + (fg_made_50_59 * 5)
            + (fg_made_60_plus * 6)
            + (fg_missed * -1)
            + (pat_made * 1)
            + (pat_missed * -1)
        , 2) as fantasy_points

    from combined

)

select * from with_fantasy_points