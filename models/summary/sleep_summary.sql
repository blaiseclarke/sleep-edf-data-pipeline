with
    epoch_data as (select * from {{ ref("sleep_metrics") }}),

    summary_metrics as (
        select
            subject_id,

            count(*) * 0.5 as total_recording_minutes,
            sum(case when sleep_stage != 'W' then 1 else 0 end)
            * 0.5 as total_sleep_minutes,

            -- how many times the subject transitioned to wake
            sum(
                case
                    when is_stage_transition = true and sleep_stage = 'W' then 1 else 0
                end
            ) as number_of_awakenings,

            -- deep sleep minutes and percentage
            sum(case when sleep_stage = 'N3' then 1 else 0 end)
            * 0.5 as deep_sleep_minutes,
            (sum(case when sleep_stage = 'N3' then 1 else 0 end) * 0.5) / nullif(
                (sum(case when sleep_stage != 'W' then 1 else 0 end) * 0.5), 0
            ) as deep_sleep_percentage,

            -- light sleep minutes and percentage
            sum(case when sleep_stage in ('N1', 'N2') then 1 else 0 end)
            * 0.5 as light_sleep_minutes,
            (
                sum(case when sleep_stage in ('N1', 'N2') then 1 else 0 end) * 0.5
            ) / nullif(
                (sum(case when sleep_stage != 'W' then 1 else 0 end) * 0.5), 0
            ) as light_sleep_percentage,

            -- REM sleep minutes and percentage
            sum(case when sleep_stage = 'REM' then 1 else 0 end)
            * 0.5 as rem_sleep_minutes,
            (sum(case when sleep_stage = 'REM' then 1 else 0 end) * 0.5) / nullif(
                (sum(case when sleep_stage != 'W' then 1 else 0 end) * 0.5), 0
            ) as rem_sleep_percentage,

            -- power metrics
            avg(delta_moving_avg) as avg_delta_power,
            avg(sigma_moving_avg) as avg_sigma_power,
            avg(beta_moving_avg) as avg_beta_power,
            avg(theta_moving_avg) as avg_theta_power,
            avg(alpha_moving_avg) as avg_alpha_power

        from epoch_data
        group by subject_id
    )

select *
from summary_metrics
