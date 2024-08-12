{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='dbt_scd_id',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}

select md5(to_char(to_timestamp(p.event_time), 'YYYY-MM-DD')) as date_key
, md5(to_char(to_timestamp(p.event_time), 'HH:MI:SS')) as time_key
, p1.position_key
, case when x.organisation_unit_id is null then {{ var("unknown_hash") }} else coalesce(o.organisation_unit_key, {{ var("unknown_hash") }}) end as organisation_unit_key
, case when coalesce(x.organisation_unit_company_code_id,'') = '' then {{ var("unknown_hash") }} else cc.company_code_key end as organisation_unit_company_code_key
, case when coalesce(x.organisation_unit_cost_center_id,'') = '' then {{ var("unknown_hash") }} else cst1.cost_center_key end as organisation_unit_cost_center_key
, case when coalesce(x.organisation_unit_contact_id,'') = '' then {{ var("unknown_hash") }} else c1.contact_key end as organisation_unit_contact_key
, case when coalesce(x.organisation_unit_location_id,'') = '' then {{ var("unknown_hash") }} else l1.location_key end as organisation_unit_location_key
, case when coalesce(p.holiday_zone_id,'') = '' then {{ var("unknown_hash") }} else h.holiday_zone_key end as holiday_zone_key 
, case when coalesce(p.job_id,'') = '' then {{ var("unknown_hash") }} else j.job_key end as job_key  
, case when coalesce(p.contact_id,'') = '' then {{ var("unknown_hash") }} else c.contact_key end as contact_key 
, case when coalesce(p.location_id,'') = '' then {{ var("unknown_hash") }} else l.location_key end as location_key   
, case when coalesce(p.cost_center_id,'') = '' then {{ var("unknown_hash") }} else cst.cost_center_key end as cost_center_key  
, case when p.current_incumbent_id is null then {{ var("unknown_hash") }} else coalesce(c2.colleague_key, {{ var("unknown_hash") }}) end as current_incumbent_key
, case when z.colleague_confidential_id is null then {{ var("unknown_hash") }} else coalesce(cc1.colleague_confidential_key, {{ var("unknown_hash") }}) end as current_incumbent_confidential_key
, case when coalesce(z.action_id,'') = '' then {{ var("unknown_hash") }} else coalesce(a.action_key, {{ var("unknown_hash") }}) end as current_incumbent_action_key
, case when coalesce(t.time_type_id,'') = '' then {{ var("unknown_hash") }} else tt.time_type_key end as current_incumbent_time_type_key
, md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'position') ) as metadata_key
, p.event_id
, p.event_time
, p.dbt_scd_id
, coalesce(p.modified_at, p.created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_position_event') }} p

asof join (select id as organisation_unit_id, company_code_id as organisation_unit_company_code_id, contact_id as organisation_unit_contact_id
            , location_id as organisation_unit_location_id, cost_center_id as organisation_unit_cost_center_id, event_time as orgunit_event_time
            , event_id as orgunit_event_id
            from {{ ref('stg_orgunit_event') }} 
          ) x 
    match_condition(p.event_time >= x.orgunit_event_time)
    on p.organisation_unit_id = x.organisation_unit_id 

asof join (select id as colleague_confidential_id, action_id, emergency_contact_id, home_contact_id, home_location_id, action_begin_date, action_changed_date, contract_type_id     
            , event_time as colleague_confidential_event_time
            , event_id as colleague_confidential_event_id
            from {{ ref('stg_colleague_confidential_event') }}
          ) z 
    match_condition(p.event_time >= z.colleague_confidential_event_time)
    on p.current_incumbent_id = z.colleague_confidential_id 

asof join (select id as colleague_id, time_type_id
            , event_time as colleague_event_time
            , event_id as colleague_event_id
            from {{ ref('stg_colleague_event') }}
          ) t 
    match_condition(p.event_time >= t.colleague_event_time)
    on p.current_incumbent_id = t.colleague_id 

left join {{ ref('dim_position') }} p1 on p.id = p1.position_id and p.event_time >= p1.dbt_valid_from and p.event_time < coalesce(p1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_job') }} j on p.job_id = j.job_id and p.event_time >= j.dbt_valid_from and p.event_time < coalesce(j.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} c on p.contact_id = c.contact_id and p.event_time >= c.dbt_valid_from and p.event_time < coalesce(c.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} l on p.location_id = l.location_id and p.event_time >= l.dbt_valid_from and p.event_time < coalesce(l.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_holiday_zone') }} h on p.holiday_zone_id = h.holiday_zone_id and p.event_time >= h.dbt_valid_from and p.event_time < coalesce(h.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_organisation_unit') }} o on x.organisation_unit_id = o.organisation_unit_id and x.orgunit_event_time >= o.dbt_valid_from and x.orgunit_event_time < coalesce(o.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_cost_center') }} cst on p.cost_center_id = cst.cost_center_id and p.event_time >= cst.dbt_valid_from and p.event_time < coalesce(cst.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_company_code') }} cc on x.organisation_unit_company_code_id = cc.company_code_id and x.orgunit_event_time >= cc.dbt_valid_from and x.orgunit_event_time < coalesce(cc.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_cost_center') }} cst1 on x.organisation_unit_cost_center_id = cst1.cost_center_id and x.orgunit_event_time >= cst1.dbt_valid_from and x.orgunit_event_time < coalesce(cst1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} c1 on x.organisation_unit_contact_id = c1.contact_id and x.orgunit_event_time >= c1.dbt_valid_from and x.orgunit_event_time < coalesce(c1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} l1 on x.organisation_unit_location_id = l1.location_id and x.orgunit_event_time >= l1.dbt_valid_from and x.orgunit_event_time < coalesce(l1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_colleague') }} c2 on p.current_incumbent_id = c2.colleague_id and p.event_time >= c2.dbt_valid_from and p.event_time < coalesce(c2.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_colleague_confidential') }} cc1 on p.current_incumbent_id = cc1.colleague_confidential_id and z.colleague_confidential_event_time >= cc1.dbt_valid_from and z.colleague_confidential_event_time < coalesce(cc1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_action') }} a on z.action_id = a.action_id and z.colleague_confidential_event_time >= a.dbt_valid_from and z.colleague_confidential_event_time < coalesce(a.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_time_type') }} tt on t.time_type_id = tt.time_type_id and t.colleague_event_time >= tt.dbt_valid_from and t.colleague_event_time < coalesce(tt.dbt_valid_to, '9999-12-31')

{% if is_incremental() %}
where coalesce(p.modified_at, p.created_at) > (select max(modified_at) from {{ this }})
{% endif %}




