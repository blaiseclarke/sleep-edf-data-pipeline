with staging as (
    select * from {{ ref('staging_sleep_data') }}
),

metrics as (
    select
    *,

    -- Deep sleep
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

    lag(sleep_stage) over (
        partition by subject_id
        order by epoch_idx
    ) as previous_stage,

    -- Was there a wake up?
    case
        when lag(sleep_stage) over (partition by subject_id order by epoch_idx) != sleep_stage
        then true
        else false
    end as is_stage_transition

    from staging
)

select * from metrics