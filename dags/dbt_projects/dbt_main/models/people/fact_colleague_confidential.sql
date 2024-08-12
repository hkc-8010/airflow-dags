{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='dbt_scd_id',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}

select md5(to_char(to_timestamp(event_time), 'YYYY-MM-DD')) as date_key
, md5(to_char(to_timestamp(event_time), 'HH:MI:SS')) as time_key
, case when f.id is null then {{ var("unknown_hash") }} else coalesce(c.colleague_key, {{ var("unknown_hash") }}) end as colleague_key
, case when coalesce(f.home_contact_id,'') = '' then {{ var("unknown_hash") }} else ccct.contact_key end as home_contact_key
, case when coalesce(f.emergency_contact_id,'') = '' then {{ var("unknown_hash") }} else ccct1.contact_key end as emergency_contact_key
, case when coalesce(f.home_location_id,'') = '' then {{ var("unknown_hash") }} else hloc.location_key end as home_location_key
, case when coalesce(f.action_id,'') = '' then {{ var("unknown_hash") }} else act.action_key end as action_key 
, case when f.id is null then {{ var("unknown_hash") }} else coalesce(cc1.colleague_confidential_key, {{ var("unknown_hash") }}) end as colleague_confidential_key
, case when f.managers_manager_id is null then {{ var("unknown_hash") }} else coalesce(mm.colleague_key, {{ var("unknown_hash") }}) end as managers_manager_key
, case when coalesce(f.action_begin_date, '') = '' then {{ var("unknown_hash") }} else coalesce(md5(to_char(to_timestamp(f.action_begin_date), 'YYYY-MM-DD')), {{ var("unknown_hash") }}) end as action_begin_date_key
, case when coalesce(f.action_changed_date, '') = '' then {{ var("unknown_hash") }} else coalesce(md5(to_char(to_timestamp(f.action_changed_date), 'YYYY-MM-DD')), {{ var("unknown_hash") }}) end as action_changed_date_key
, case when coalesce(f.contract_type_id,'') = '' then {{ var("unknown_hash") }} else ct.contract_type_key end as contract_type_key

, md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'colleague') ) as metadata_key
, event_id
, event_time
, f.dbt_scd_id
, coalesce(f.modified_at, f.created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_colleague_confidential_event') }} f
    
left join {{ ref('dim_colleague') }} c on f.id = c.colleague_id and f.event_time >= c.dbt_valid_from and f.event_time < coalesce(c.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} ccct on f.home_contact_id = ccct.contact_id and f.event_time >= ccct.dbt_valid_from and f.event_time < coalesce(ccct.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} ccct1 on f.emergency_contact_id = ccct1.contact_id and f.event_time >= ccct1.dbt_valid_from and f.event_time < coalesce(ccct1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_action') }} act on f.action_id = act.action_id and f.event_time >= act.dbt_valid_from and f.event_time < coalesce(act.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_colleague_confidential') }} cc1 on f.id = cc1.colleague_confidential_id and f.event_time >= cc1.dbt_valid_from and f.event_time < coalesce(cc1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_colleague') }} mm on f.managers_manager_id = mm.colleague_id and f.event_time >= mm.dbt_valid_from and f.event_time < coalesce(mm.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contract_type') }} ct on f.contract_type_id = ct.contract_type_id and f.event_time >= ct.dbt_valid_from and f.event_time < coalesce(ct.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} hloc on f.home_location_id = hloc.location_id and f.event_time >= hloc.dbt_valid_from and f.event_time < coalesce(hloc.dbt_valid_to, '9999-12-31')



{% if is_incremental() %}
where coalesce(f.modified_at, f.created_at) > (select max(modified_at) from {{ this }})
{% endif %}


