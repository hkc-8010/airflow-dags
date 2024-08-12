{{
    config(
        materialized='incremental',
        unique_key='dbt_scd_id',
        merge_exclude_columns = ['created_at']
    )
}}

select id
, case when action_type_id is not null and action_reason_id is not null then action_type_id || '_' || action_reason_id end as action_id
, emergency_contact_id
, home_contact_id
, home_location_id
, contract_type_id      
, action_begin_date                     
, action_changed_date     
, managers_manager_id              
, event_id
, event_type
, event_time
, event_source
, event_subject
, current_timestamp() as created_at
, current_timestamp() as modified_at
, md5(concat(coalesce(cast(id as string),''), coalesce(cast(to_timestamp(event_time) as string),''))) as dbt_scd_id
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_colleague_confidential_data_event') }}


{% if is_incremental() %}

where
  event_time > (select max(event_time) from {{ this }})

{% endif %}