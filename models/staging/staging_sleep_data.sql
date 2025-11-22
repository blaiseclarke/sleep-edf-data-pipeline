with source as (
    select * from {{ source('eeg_raw', 'SLEEP_EPOCHS') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['subject_id', 'epoch_idx']) }} as epoch_id,

        coalesce(cast(subject_id as varchar), '1') as subject_id,
        cast(epoch_idx as int) as epoch_idx,
        cast(stage as varchar) as sleep_stage,
        
        cast(delta_power as float) as delta_power_uv,
        cast(theta_power as float) as theta_power_uv,
        cast(alpha_power as float) as alpha_power_uv,
        cast(sigma_power as float) as sigma_power_uv,
        cast(beta_power as float) as beta_power_uv

    from source
)

select * from cleaned