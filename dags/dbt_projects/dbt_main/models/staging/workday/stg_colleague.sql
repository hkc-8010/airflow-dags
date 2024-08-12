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
-- NB. this scd incremental approach relies on the key of your table named as "id"
{% set column_names =  ['id', 'full_name','colleague_status','contracted_hours','email_address','full_time_equivalent','hire_date','initials','is_financial_advisor','is_fire_warden','is_first_aid_officer','is_health_and_safety_rep','is_nab_staff_accessing_bnz_systems','is_operational','is_people_leader','legacy_colleague_id','legal_name', 'legal_first_name','legal_middle_name', 'legal_last_name','manager_id','organisation_assignment_end_date','preferred_first_name','preferred_last_name','user_id','deleted'] %}

with source_rows_base as (
  select id
  , full_name
  , colleague_status
  , contracted_hours
  , email_address
  , full_time_equivalent
  , hire_date
  , initials
  , is_financial_advisor
  , is_fire_warden
  , is_first_aid_officer
  , is_health_and_safety_rep
  , is_nab_staff_accessing_bnz_systems
  , is_operational
  , is_people_leader
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
  , event_id
  , event_time
  , 'colleague' as event_msg_source
  , current_timestamp() as created_at
  , current_timestamp() as modified_at
  , deleted
  from {{ ref('stg_colleague_data_event') }}

)

{{ generate_scd_incremental(column_names, is_incremental()) }}

