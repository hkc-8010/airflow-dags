{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='colleague_confidential_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as colleague_confidential_key
, id as colleague_confidential_id
, contract_group
, annual_leave_balance
, annual_salary_amount
, compensation_start_date
, contract_start_date
, contract_expiry_date
, date_of_birth
, gender_identity
, kiwisaver_employee_contribution
, kiwisaver_employer_contribution
, pay_currency
, pay_scale_level
, simplified_group
, superannuation_employee_contribution
, superannuation_employer_contribution
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, deleted
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_colleague_confidential') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}
