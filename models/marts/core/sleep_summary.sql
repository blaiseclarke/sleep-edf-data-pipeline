{% set epoch_minutes = var("epoch_length_seconds") / 60.0 %}

with
    epoch_data as (select * from {{ ref("sleep_metrics") }}),

    -- Count every epoch class once, then convert to minutes in the next CTE.
    -- Every metric except the recording length is scoped to the main sleep
    -- episode, since these recordings span a whole day and often contain naps
    -- (see sleep_metrics for how the window is derived).
    epoch_counts as (
        select
            subject_id,

            count(*) as recording_epochs,
            sum(case when is_in_sleep_period then 1 else 0 end) as sleep_period_epochs,

            sum(
                case
                    when
                        is_in_sleep_period
                        and sleep_stage in ('N1', 'N2', 'N3', 'REM')
                    then 1
                    else 0
                end
            ) as sleep_epochs,
            sum(
                case when is_in_sleep_period and sleep_stage = 'N3' then 1 else 0 end
            ) as deep_epochs,
            sum(
                case
                    when is_in_sleep_period and sleep_stage in ('N1', 'N2') then 1 else 0
                end
            ) as light_epochs,
            sum(
                case when is_in_sleep_period and sleep_stage = 'REM' then 1 else 0 end
            ) as rem_epochs,

            -- Wake after sleep onset: wake epochs falling inside the sleep period
            sum(
                case when is_in_sleep_period and sleep_stage = 'W' then 1 else 0 end
            ) as waso_epochs,

            -- Only transitions into wake within the sleep period are awakenings;
            -- counting them across the full recording captures the waking day too
            sum(
                case
                    when
                        is_in_sleep_period
                        and is_stage_transition
                        and sleep_stage = 'W'
                    then 1
                    else 0
                end
            ) as awakening_count,

            -- Band power averaged over the sleep period only, so that hours of
            -- daytime wakefulness do not dominate the spectral profile
            avg(
                case when is_in_sleep_period then delta_moving_avg end
            ) as avg_delta_power,
            avg(
                case when is_in_sleep_period then sigma_moving_avg end
            ) as avg_sigma_power,
            avg(case when is_in_sleep_period then beta_moving_avg end) as avg_beta_power,
            avg(
                case when is_in_sleep_period then theta_moving_avg end
            ) as avg_theta_power,
            avg(
                case when is_in_sleep_period then alpha_moving_avg end
            ) as avg_alpha_power

        from epoch_data
        group by subject_id
    ),

    summary_metrics as (
        select
            subject_id,

            -- Duration of the whole recording, waking day included
            recording_epochs * {{ epoch_minutes }} as total_recording_minutes,

            -- Sleep onset through final awakening: the proxy for time in bed
            sleep_period_epochs * {{ epoch_minutes }} as sleep_period_minutes,

            sleep_epochs * {{ epoch_minutes }} as total_sleep_minutes,
            waso_epochs * {{ epoch_minutes }} as waso_minutes,

            awakening_count as number_of_awakenings,

            -- Total sleep time over time in bed
            sleep_epochs
            / nullif(cast(sleep_period_epochs as float), 0) as sleep_efficiency,

            -- Stage minutes, and each stage as a share of total sleep time
            deep_epochs * {{ epoch_minutes }} as deep_sleep_minutes,
            deep_epochs
            / nullif(cast(sleep_epochs as float), 0) as deep_sleep_percentage,

            light_epochs * {{ epoch_minutes }} as light_sleep_minutes,
            light_epochs
            / nullif(cast(sleep_epochs as float), 0) as light_sleep_percentage,

            rem_epochs * {{ epoch_minutes }} as rem_sleep_minutes,
            rem_epochs / nullif(cast(sleep_epochs as float), 0) as rem_sleep_percentage,

            avg_delta_power,
            avg_sigma_power,
            avg_beta_power,
            avg_theta_power,
            avg_alpha_power

        from epoch_counts
    )

select *
from summary_metrics
