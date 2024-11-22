{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}
 
select
    {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as user_key,
    employee_id,
    employee_name,
    {{custom_calc('department_id')}} as dept_id,
    email,
    phone,
    address,
    hire_date,
    employee_status,
    md5_column,
    current_timestamp as sf_insert_dttm,
    current_timestamp as sf_update_dttm
from {{ ref('stg_dim_user') }}
 
 
{% if is_incremental() %}
 
  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  where md5_column not in (select md5_column  from {{ this }})
 
{% endif %}