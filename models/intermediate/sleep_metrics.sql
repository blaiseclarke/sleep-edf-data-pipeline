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
                    != sleep_stage
                then true
                else false
            end as is_stage_transition

        from staging
    )

select *
from metrics
