with
    staging as (select * from {{ ref("staging_sleep_data") }}),

    metrics as (
        select
            *,

            -- Deep sleep (delta) logic
            -- Use a rolling average over the last 5 epochs (2.5 minutes)
            -- Smoothing gives the trend of deep sleep
            avg(delta_power_uv) over (
                partition by subject_id
                order by epoch_idx
                rows between 4 preceding and current row
            ) as delta_moving_avg,

            -- Light sleep
            avg(sigma_power_uv) over (
                partition by subject_id
                order by epoch_idx
                rows between 4 preceding and current row
            ) as sigma_moving_avg,

            -- Active wake
            avg(beta_power_uv) over (
                partition by subject_id
                order by epoch_idx
                rows between 4 preceding and current row
            ) as beta_moving_avg,

            -- N1/REM
            avg(theta_power_uv) over (
                partition by subject_id
                order by epoch_idx
                rows between 4 preceding and current row
            ) as theta_moving_avg,

            -- Wake
            avg(alpha_power_uv) over (
                partition by subject_id
                order by epoch_idx
                rows between 4 preceding and current row
            ) as alpha_moving_avg,

            -- Transition detection
            -- Look at the previous row (LAG) and compare it to current
            -- If they are different (e.g., N2 -> N3), that's a transition
            -- High transition counts can indicate fragmented sleep
            case
                when
                    lag(sleep_stage) over (partition by subject_id order by epoch_idx)
                    is null
                then false
                when
                    lag(sleep_stage) over (partition by subject_id order by epoch_idx)
                    != sleep_stage
                then true
                else false
            end as is_stage_transition

        from staging
    ),

    -- Sleep period detection
    --
    -- Sleep-EDF recordings are ~22h ambulatory recordings spanning a full day,
    -- so whole-recording aggregates are dominated by ordinary daytime
    -- wakefulness rather than by the night. Worse, many subjects nap, so simply
    -- bounding by the first and last sleep epoch can span nearly the entire
    -- recording and count an evening awake in front of the TV as WASO.
    --
    -- Instead, split each recording into sleep episodes wherever a continuous
    -- wake bout runs longer than `sleep_episode_gap_minutes`, then keep the
    -- episode containing the most sleep. Downstream marts scope their metrics
    -- to that window, so awakenings, WASO, and band power describe the main
    -- sleep episode. The dataset carries no lights-off annotation, so this is
    -- the closest available proxy for time in bed.
    flagged as (
        select *, case when sleep_stage = 'W' then 0 else 1 end as is_sleep
        from metrics
    ),

    -- Gaps and islands: rows in one contiguous same-state run share a run_key
    runs as (
        select
            *,
            row_number() over (partition by subject_id order by epoch_idx)
            - row_number() over (
                partition by subject_id, is_sleep order by epoch_idx
            ) as run_key
        from flagged
    ),

    breaks as (
        select
            *,
            case
                when
                    is_sleep = 0
                    and count(*) over (
                        partition by subject_id, is_sleep, run_key
                    ) >= {{ (var("sleep_episode_gap_minutes") * 60) / var("epoch_length_seconds") }}
                then 1
                else 0
            end as is_episode_break
        from runs
    ),

    episodes as (
        select
            *,
            sum(is_episode_break) over (
                partition by subject_id
                order by epoch_idx
                rows between unbounded preceding and current row
            ) as episode_id
        from breaks
    ),

    -- Rank episodes by how much sleep they contain; episode_id breaks ties so
    -- that a subject with two equal episodes still resolves to exactly one
    ranked_episodes as (
        select
            subject_id,
            episode_id,
            row_number() over (
                partition by subject_id order by sum(is_sleep) desc, episode_id
            ) as episode_rank
        from episodes
        group by subject_id, episode_id
        having sum(is_sleep) > 0
    ),

    bounds as (
        select
            e.subject_id,
            min(case when e.is_sleep = 1 then e.epoch_idx end) as sleep_onset_epoch_idx,
            max(
                case when e.is_sleep = 1 then e.epoch_idx end
            ) as final_awakening_epoch_idx
        from episodes as e
        join
            ranked_episodes as r
            on e.subject_id = r.subject_id
            and e.episode_id = r.episode_id
            and r.episode_rank = 1
        group by e.subject_id
    )

select
    e.epoch_id,
    e.subject_id,
    e.epoch_idx,
    e.sleep_stage,

    e.delta_power_uv,
    e.theta_power_uv,
    e.alpha_power_uv,
    e.sigma_power_uv,
    e.beta_power_uv,

    e.delta_moving_avg,
    e.sigma_moving_avg,
    e.beta_moving_avg,
    e.theta_moving_avg,
    e.alpha_moving_avg,

    e.is_stage_transition,

    b.sleep_onset_epoch_idx,
    b.final_awakening_epoch_idx,

    -- Subjects with no scored sleep have null bounds, and so an empty window
    coalesce(
        e.epoch_idx between b.sleep_onset_epoch_idx and b.final_awakening_epoch_idx,
        false
    ) as is_in_sleep_period

from episodes as e
left join bounds as b on e.subject_id = b.subject_id
