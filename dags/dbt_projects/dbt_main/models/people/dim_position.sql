{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='position_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as position_key
, id as position_id
, position_description
, position_business_description
, position_start_date
, position_end_date
, colleague_position_start_date
, colleague_position_end_date
, banker_flag
, is_chief_position as chief_position_flag
, is_wsb as wsb_flag
, is_people_leader as people_leader_flag
, lending_role_flag
, customer_facing_role_flag
, dca_holder_flag
, store_based_role_flag
, manage_customer_role_flag
, nominated_representative_flag
, responsible_person_flag
, purchasing_approval_level
, job_level
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, deleted
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_position') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}



