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
, md5(to_char(to_timestamp(o.event_time), 'HH:MI:SS')) as time_key
, ou.organisation_unit_key
, case when coalesce(o.company_code_id,'') = '' then {{ var("unknown_hash") }} else cc.company_code_key end as company_code_key
, case when coalesce(o.contact_id,'') = '' then {{ var("unknown_hash") }} else c.contact_key end as contact_key
, case when coalesce(o.location_id,'') = '' then {{ var("unknown_hash") }} else l.location_key end as location_key
, case when coalesce(o.region_id,'') = '' then {{ var("unknown_hash") }} else r.region_key end as region_key
, case when coalesce(o.country_id,'') = '' then {{ var("unknown_hash") }} else cou.country_key end as country_key
, case when coalesce(o.cost_center_id,'') = '' then {{ var("unknown_hash") }} else cst.cost_center_key end as cost_center_key
, md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'orgunit') ) as metadata_key
, o.event_id
, o.event_time
, o.dbt_scd_id
, coalesce(o.modified_at, o.created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_orgunit_event') }} o
left join {{ ref('dim_company_code') }} cc on cc.company_code_id = o.company_code_id and o.event_time >= cc.dbt_valid_from and o.event_time < coalesce(cc.dbt_valid_to, '9999-12-31')
left join  {{ ref('dim_contact') }} c on c.contact_id = o.contact_id and o.event_time >= c.dbt_valid_from and o.event_time < coalesce(c.dbt_valid_to, '9999-12-31')
left join  {{ ref('dim_location') }} l on l.location_id = o.location_id and o.event_time >= l.dbt_valid_from and o.event_time < coalesce(l.dbt_valid_to, '9999-12-31')
left join  {{ ref('dim_region') }} r on r.region_id = o.region_id and o.event_time >= r.dbt_valid_from and o.event_time < coalesce(r.dbt_valid_to, '9999-12-31')
left join  {{ ref('dim_country') }} cou on cou.country_id = o.country_id and o.event_time >= cou.dbt_valid_from and o.event_time < coalesce(cou.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_organisation_unit') }} ou on o.id = ou.organisation_unit_id and o.event_time >= ou.dbt_valid_from and o.event_time < coalesce(ou.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_cost_center') }} cst on o.cost_center_id = cst.cost_center_id and o.event_time >= cst.dbt_valid_from and o.event_time < coalesce(cst.dbt_valid_to, '9999-12-31')

{% if is_incremental() %}
where coalesce(o.modified_at, o.created_at) > (select max(modified_at) from {{ this }})
{% endif %}