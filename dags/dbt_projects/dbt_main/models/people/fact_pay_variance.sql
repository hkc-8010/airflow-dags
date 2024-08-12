{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='pay_variance_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}

select f.id as pay_variance_key
, case when f.employee_id is null then {{ var("unknown_hash") }} else coalesce(c.colleague_key, {{ var("unknown_hash") }}) end as colleague_key
, md5(to_char(to_timestamp(f.file_time), 'YYYY-MM-DD')) as date_key
, md5(to_char(to_timestamp(f.file_time), 'HH:MI:SS')) as time_key
, case when coalesce(f.wage_type_code,'') = '' then {{ var("unknown_hash") }} else wt.wage_type_key end as wage_type_key
, case when coalesce(f.included_in_gross, '') || '|' || coalesce(f.finalised, '') || '|' ||  coalesce(f.retro, '') = '||' then {{ var("unknown_hash") }} else pva.pay_variance_attributes_key end as pay_variance_attributes_key
, case when coalesce(f.pay_unit_type,'') = '' then {{ var("unknown_hash") }} else put.pay_unit_type_key end as pay_unit_type_key 
, md5(to_char(to_date(f.pay_period_start_date), 'YYYY-MM-DD')) as pay_period_start_date_key
, md5(to_char(to_date(f.pay_period_end_date), 'YYYY-MM-DD')) as pay_period_end_date_key
, case when f.for_pay_period_start_date is null then {{ var("unknown_hash") }} else md5(coalesce(cast(f.for_pay_period_start_date as string),'')) end as for_pay_period_start_date_key
, case when f.for_pay_period_end_date is null then {{ var("unknown_hash") }} else md5(coalesce(cast(f.for_pay_period_end_date as string),'')) end as for_pay_period_end_date_key
, md5(concat(coalesce(cast(to_timestamp(f.file_time) as string),''), 'pay_variance') ) as metadata_key
, f.pay_quantity as quantity
, f.pay_amount as amount
, f.file_name
, f.file_time
, coalesce(f.modified_at, f.created_at) as dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from {{ ref('stg_pay_variance') }} f
left join {{ ref('dim_colleague') }} c on f.employee_id = c.colleague_id and f.file_time >= c.dbt_valid_from and f.file_time < coalesce(c.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_wage_type') }} wt on f.wage_type_code = wt.wage_type_id and f.file_time >= wt.dbt_valid_from and f.file_time < coalesce(wt.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_pay_variance_attributes') }} pva on coalesce(f.included_in_gross, '') || '|' || coalesce(f.finalised, '') || '|' ||  coalesce(f.retro, '') = pva.pay_variance_attributes_id
            and f.file_time >= pva.dbt_valid_from and f.file_time < coalesce(pva.dbt_valid_to, '9999-12-31')
left join {{ ref('dim_pay_unit_type') }} put on f.pay_unit_type = put.pay_unit_type_id and f.file_time >= put.dbt_valid_from and f.file_time < coalesce(put.dbt_valid_to, '9999-12-31')


{% if is_incremental() %}
where coalesce(f.modified_at, f.created_at) > (select max(modified_at) from {{ this }})
{% endif %}