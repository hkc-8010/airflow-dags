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
, c.colleague_key
, case when f.manager_id is null then {{ var("unknown_hash") }} else coalesce(m.colleague_key, {{ var("unknown_hash") }}) end as manager_key
, case when t.manager_position_id is null then {{ var("unknown_hash") }} else coalesce(mp.position_key, {{ var("unknown_hash") }}) end as manager_position_key    -- TBC This was in colleague but now managerPosition is in Position msg…but data not seen yet.
, case when coalesce(f.country_group_id,'') = '' then {{ var("unknown_hash") }} else cg.country_group_key end as country_group_key
, case when coalesce(f.holiday_zone_id,'') = '' then {{ var("unknown_hash") }} else hz.holiday_zone_key end as holiday_zone_key
, case when f.organisation_unit_id is null then {{ var("unknown_hash") }} else coalesce(ou.organisation_unit_key, {{ var("unknown_hash") }}) end as organisation_unit_key

, case when coalesce(x.organisation_unit_company_code_id,'') = '' then {{ var("unknown_hash") }} else cc.company_code_key end as organisation_unit_company_code_key
, case when coalesce(x.organisation_unit_cost_center_id,'') = '' then {{ var("unknown_hash") }} else cst.cost_center_key end as organisation_unit_cost_center_key
, case when coalesce(x.organisation_unit_contact_id,'') = '' then {{ var("unknown_hash") }} else ct.contact_key end as organisation_unit_contact_key
, case when coalesce(x.organisation_unit_location_id,'') = '' then {{ var("unknown_hash") }} else loc.location_key end as organisation_unit_location_key
, case when f.position_id is null then {{ var("unknown_hash") }} else coalesce(p.position_key, {{ var("unknown_hash") }}) end as position_key

, case when y.position_organisation_unit_id is null then {{ var("unknown_hash") }} else coalesce(pou.organisation_unit_key, {{ var("unknown_hash") }}) end as position_organisation_unit_key
, case when coalesce(y.position_holiday_zone_id,'') = '' then {{ var("unknown_hash") }} else phz.holiday_zone_key end as position_holiday_zone_key
, case when coalesce(y.position_contact_id,'') = '' then {{ var("unknown_hash") }} else pct.contact_key end as position_contact_key
, case when coalesce(y.position_location_id,'') = '' then {{ var("unknown_hash") }} else ploc.location_key end as position_location_key
, case when coalesce(y.position_cost_center_id,'') = '' then {{ var("unknown_hash") }} else pcst.cost_center_key end as position_cost_center_key
, case when coalesce(y.position_job_id,'') = '' then {{ var("unknown_hash") }} else pjb.job_key end as position_job_key

, case when coalesce(f.work_contact_id,'') = '' then {{ var("unknown_hash") }} else wct.contact_key end as work_contact_key
, case when coalesce(f.work_location_id,'') = '' then {{ var("unknown_hash") }} else wloc.location_key end as work_location_key
, case when coalesce(f.cost_center_id,'') = '' then {{ var("unknown_hash") }} else cst1.cost_center_key end as cost_center_key

, case when coalesce(z.home_contact_id,'') = '' then {{ var("unknown_hash") }} else ccct.contact_key end as home_contact_key
, case when coalesce(z.home_location_id,'') = '' then {{ var("unknown_hash") }} else hloc.location_key end as home_location_key
, case when coalesce(z.emergency_contact_id,'') = '' then {{ var("unknown_hash") }} else ccct1.contact_key end as emergency_contact_key
, case when coalesce(z.action_id,'') = '' then {{ var("unknown_hash") }} else act.action_key end as action_key 
, case when coalesce(z.contract_type_id,'') = '' then {{ var("unknown_hash") }} else ctt.contract_type_key end as contract_type_key 
, case when z.colleague_confidential_id is null then {{ var("unknown_hash") }} else coalesce(cc1.colleague_confidential_key, {{ var("unknown_hash") }}) end as colleague_confidential_key

, case when coalesce(z.action_begin_date, '') = '' then {{ var("unknown_hash") }} else coalesce(md5(to_char(to_timestamp(z.action_begin_date), 'YYYY-MM-DD')), {{ var("unknown_hash") }}) end as action_begin_date_key
, case when coalesce(z.action_changed_date, '') = '' then {{ var("unknown_hash") }} else coalesce(md5(to_char(to_timestamp(z.action_changed_date), 'YYYY-MM-DD')), {{ var("unknown_hash") }}) end as action_changed_date_key

, case when coalesce(f.employee_type_id,'') = '' then {{ var("unknown_hash") }} else et.employee_type_key end as employee_type_key
, case when coalesce(f.time_type_id,'') = '' then {{ var("unknown_hash") }} else tt.time_type_key end as time_type_key

, md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'colleague') ) as metadata_key
, event_id
, event_time
, f.dbt_scd_id
, coalesce(f.modified_at, f.created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_colleague_event') }} f
asof join (select id as position_id, organisation_unit_id as position_organisation_unit_id, holiday_zone_id as position_holiday_zone_id, contact_id as position_contact_id
            , location_id as position_location_id, job_id as position_job_id
            , cost_center_id as position_cost_center_id
            , event_time as position_event_time
            , event_id as position_event_id
            from {{ ref('stg_position_event') }}
          ) y 
    match_condition(f.event_time >= y.position_event_time)
    on f.position_id = y.position_id 

asof join (select id as organisation_unit_id, company_code_id as organisation_unit_company_code_id, contact_id as organisation_unit_contact_id
            , location_id as organisation_unit_location_id, cost_center_id as organisation_unit_cost_center_id, event_time as orgunit_event_time
            , event_id as orgunit_event_id
            from {{ ref('stg_orgunit_event') }}
          ) x 
    match_condition(f.event_time >= x.orgunit_event_time)
    on f.organisation_unit_id = x.organisation_unit_id 

asof join (select id as colleague_confidential_id, action_id, emergency_contact_id, home_contact_id, home_location_id, action_begin_date, action_changed_date, contract_type_id     
            , event_time as colleague_confidential_event_time
            , event_id as colleague_confidential_event_id
            from {{ ref('stg_colleague_confidential_event') }}
          ) z 
    match_condition(f.event_time >= z.colleague_confidential_event_time)
    on f.id = z.colleague_confidential_id 

asof join (select id as manager_colleague_id, position_id as manager_position_id 
            , event_time as manager_colleague_event_time
            , event_id as manager_colleague_event_id
            from {{ ref('stg_colleague_event') }}
          ) t 
    match_condition(f.event_time >= t.manager_colleague_event_time)
    on f.manager_id = t.manager_colleague_id 
       
left join {{ ref('dim_colleague') }} c on f.id = c.colleague_id and f.event_time >= c.dbt_valid_from and f.event_time < coalesce(c.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_colleague') }} m on f.manager_id = m.colleague_id and f.event_time >= m.dbt_valid_from and f.event_time < coalesce(m.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_country_group') }} cg on f.country_group_id = cg.country_group_id and f.event_time >= cg.dbt_valid_from and f.event_time < coalesce(cg.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_holiday_zone') }} hz on f.holiday_zone_id = hz.holiday_zone_id and f.event_time >= hz.dbt_valid_from and f.event_time < coalesce(hz.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_organisation_unit') }} ou on f.organisation_unit_id = ou.organisation_unit_id and f.event_time >= ou.dbt_valid_from and f.event_time < coalesce(ou.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_company_code') }} cc on x.organisation_unit_company_code_id = cc.company_code_id and x.orgunit_event_time >= cc.dbt_valid_from and x.orgunit_event_time < coalesce(cc.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_cost_center') }} cst on x.organisation_unit_cost_center_id = cst.cost_center_id and x.orgunit_event_time >= cst.dbt_valid_from and x.orgunit_event_time < coalesce(cst.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} ct on x.organisation_unit_contact_id = ct.contact_id and x.orgunit_event_time >= ct.dbt_valid_from and x.orgunit_event_time < coalesce(ct.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} loc on x.organisation_unit_location_id = loc.location_id and x.orgunit_event_time >= loc.dbt_valid_from and x.orgunit_event_time < coalesce(loc.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_position') }} p on f.position_id = p.position_id and f.event_time >= p.dbt_valid_from and f.event_time < coalesce(p.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_organisation_unit') }} pou on y.position_organisation_unit_id = pou.organisation_unit_id and y.position_event_time >= pou.dbt_valid_from and y.position_event_time < coalesce(pou.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_holiday_zone') }} phz on y.position_holiday_zone_id = phz.holiday_zone_id and y.position_event_time >= phz.dbt_valid_from and y.position_event_time < coalesce(phz.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} pct on y.position_contact_id = pct.contact_id and y.position_event_time >= pct.dbt_valid_from and y.position_event_time < coalesce(pct.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} ploc on y.position_location_id = ploc.location_id and y.position_event_time >= ploc.dbt_valid_from and y.position_event_time < coalesce(ploc.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_cost_center') }} pcst on y.position_cost_center_id = pcst.cost_center_id and y.position_event_time >= pcst.dbt_valid_from and y.position_event_time < coalesce(pcst.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_job') }} pjb on y.position_job_id = pjb.job_id and y.position_event_time >= pjb.dbt_valid_from and y.position_event_time < coalesce(pjb.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_contact') }} wct on f.work_contact_id = wct.contact_id and f.event_time >= wct.dbt_valid_from and f.event_time < coalesce(wct.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} wloc on f.work_location_id = wloc.location_id and f.event_time >= wloc.dbt_valid_from and f.event_time < coalesce(wloc.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_location') }} hloc on z.home_location_id = hloc.location_id and z.colleague_confidential_event_time >= hloc.dbt_valid_from and z.colleague_confidential_event_time < coalesce(hloc.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_cost_center') }} cst1 on f.cost_center_id = cst1.cost_center_id and f.event_time >= cst1.dbt_valid_from and f.event_time < coalesce(cst1.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_contact') }} ccct on z.home_contact_id = ccct.contact_id and z.colleague_confidential_event_time >= ccct.dbt_valid_from and z.colleague_confidential_event_time < coalesce(ccct.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contact') }} ccct1 on z.emergency_contact_id = ccct1.contact_id and z.colleague_confidential_event_time >= ccct1.dbt_valid_from and z.colleague_confidential_event_time < coalesce(ccct1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_action') }} act on z.action_id = act.action_id and z.colleague_confidential_event_time >= act.dbt_valid_from and z.colleague_confidential_event_time < coalesce(act.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_colleague_confidential') }} cc1 on z.colleague_confidential_id = cc1.colleague_confidential_id and z.colleague_confidential_event_time >= cc1.dbt_valid_from and z.colleague_confidential_event_time < coalesce(cc1.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_contract_type') }} ctt on z.contract_type_id = ctt.contract_type_id and z.colleague_confidential_event_time >= ctt.dbt_valid_from and z.colleague_confidential_event_time < coalesce(ctt.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_position') }} mp on t.manager_position_id = mp.position_id and t.manager_colleague_event_time >= mp.dbt_valid_from and t.manager_colleague_event_time < coalesce(mp.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_employee_type') }} et on f.employee_type_id = et.employee_type_id and f.event_time >= et.dbt_valid_from and f.event_time < coalesce(et.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_time_type') }} tt on f.time_type_id = tt.time_type_id and f.event_time >= tt.dbt_valid_from and f.event_time < coalesce(tt.dbt_valid_to, '9999-12-31')

{% if is_incremental() %}
where coalesce(f.modified_at, f.created_at) > (select max(modified_at) from {{ this }})
{% endif %}


