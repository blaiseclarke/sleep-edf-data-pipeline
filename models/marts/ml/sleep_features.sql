with
    metrics as (select * from {{ ref("sleep_metrics")}}),

    -- Calculate biomarker ratios
    power_ratios as (
        select
        *,
        delta_moving_avg / nullif(beta_moving_avg, 0) as delta_beta_ratio,
        delta_moving_avg / nullif(alpha_moving_avg, 0) as delta_alpha_ratio,
        theta_moving_avg / nullif(alpha_moving_avg, 0) as theta_alpha_ratio

        from metrics
    ),

    -- Normalize power ratios per subject

    normalized as (
        select
        epoch_id,
        subject_id,
        sleep_stage,

        (delta_beta_ratio - avg(delta_beta_ratio) over (partition by subject_id))
        / nullif(stddev(delta_beta_ratio) over (partition by subject_id), 0)
        as delta_beta_ratio_z,

        delta_beta_ratio,

        (delta_alpha_ratio - avg(delta_alpha_ratio) over (partition by subject_id))
        / nullif(stddev(delta_alpha_ratio) over (partition by subject_id), 0)
        as delta_alpha_ratio_z,

        delta_alpha_ratio,

        (theta_alpha_ratio - avg(theta_alpha_ratio) over (partition by subject_id))
        / nullif(stddev(theta_alpha_ratio) over (partition by subject_id), 0)
        as theta_alpha_ratio_z,

        theta_alpha_ratio

        from power_ratios
    )

select * from normalized