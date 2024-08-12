{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='colleague_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}


select dbt_scd_id as colleague_key
, id as colleague_id
, colleague_status as status
, contracted_hours
, email_address
, full_name
, full_time_equivalent
, hire_date
, initials
, is_financial_advisor as financial_advisor_flag
, is_fire_warden as fire_warden_flag
, is_first_aid_officer as first_aid_officer_flag
, is_health_and_safety_rep as health_and_safety_rep_flag
, is_nab_staff_accessing_bnz_systems as nab_staff_accessing_bnz_systems_flag
, is_operational as operational_flag
, is_people_leader as people_leader_flag
, legacy_colleague_id
, legal_name
, legal_first_name
, legal_middle_name
, legal_last_name
, manager_id
, organisation_assignment_end_date
, preferred_first_name
, preferred_last_name
, user_id
, dbt_valid_from
, dbt_valid_to
, coalesce(modified_at, created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, deleted
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_colleague') }}

{% if is_incremental() %}
where coalesce(modified_at, created_at) > (select max(modified_at) from {{ this }})
{% endif %}
