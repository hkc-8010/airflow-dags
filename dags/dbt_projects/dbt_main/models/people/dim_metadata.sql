{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='metadata_key',
        tags = ["main_processing"],
        merge_exclude_columns = ['created_at']
    )
}}

with cte_metadata as (
    select md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'orgunit') ) as metadata_key
    , event_id
    , event_type
    , event_time
    , event_source
    , event_subject
    , 'orgunit' as event_msg_source
    , coalesce(modified_at, created_at) as dbt_updated_at
    from {{ ref('stg_orgunit_event') }}

    union

    select md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'position') ) as metadata_key
    , event_id
    , event_type
    , event_time
    , event_source
    , event_subject
    , 'position' as event_msg_source
    , coalesce(modified_at, created_at) as dbt_updated_at
    from {{ ref('stg_position_event') }}

    union

    select md5(concat(coalesce(cast(event_id as string),''), coalesce(cast(to_timestamp(event_time) as string),''), 'colleague') ) as metadata_key
    , event_id
    , event_type
    , event_time
    , event_source
    , event_subject
    , 'colleague' as event_msg_source
    , coalesce(modified_at, created_at) as dbt_updated_at
    from {{ ref('stg_colleague_event') }}

    union

    select md5(concat(coalesce(cast(to_timestamp(file_time) as string),''), 'pay_variance') ) as metadata_key
    , min(id) as event_id
    , 'FILE' as event_type
    , file_time as event_time
    , file_name as event_source
    , 'TODO' as event_subject
    , 'pay_variance' as event_msg_source
    , coalesce(modified_at, created_at) as dbt_updated_at
    from {{ ref('stg_pay_variance') }}
    group by all

)
select metadata_key
, event_id
, event_type
, event_time
, event_source
, event_subject
, event_msg_source
, dbt_updated_at
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from cte_metadata

{% if is_incremental() %}
where dbt_updated_at > (select max(modified_at) from {{ this }})
{% endif %}



