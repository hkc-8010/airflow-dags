{{ 
    config(
        materialized='incremental',
        unique_key="md5(concat(coalesce(cast(id as string),''), coalesce(cast(to_timestamp(event_time) as string),'')))",
        incremental_strategy='delete+insert',
        merge_exclude_columns=['created_at'],
        incremental_predicates=["dbt_valid_to is null"],
        post_hook=["update {{ this }} set modified_at=current_timestamp() where modified_at is null and dbt_valid_to is not null"]
    ) 
}}

-- specify your column names from source_rows_base (not including the event meta) so we can inject into the re-useable logic housed in generate_scd_incremental...
-- NB. this scd incremental approach relies on the key of your table named as "id" AND the second column as varchar to hold the 'Unknown' member value (generate_scd_incremental was designed for dims that hold and ID & DESCRIPTION at a minimum)
{% set column_names =  ['id', 'contract_group', 'annual_leave_balance','annual_salary_amount', 'compensation_start_date', 'contract_start_date', 'contract_expiry_date', 'date_of_birth', 'gender_identity', 'kiwisaver_employee_contribution', 'kiwisaver_employer_contribution', 'pay_currency', 'pay_scale_level','simplified_group','superannuation_employee_contribution', 'superannuation_employer_contribution', 'deleted'] %}

with source_rows_base as (
  select id
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
  , managers_manager_id
  , pay_currency
  , pay_scale_level
  , simplified_group
  , superannuation_employee_contribution
  , superannuation_employer_contribution
  , event_id
  , event_time
  , 'colleague_confidential' as event_msg_source
  , current_timestamp() as created_at
  , current_timestamp() as modified_at
  , deleted
  from {{ ref('stg_colleague_confidential_data_event') }}

)

{{ generate_scd_incremental(column_names, is_incremental()) }}

