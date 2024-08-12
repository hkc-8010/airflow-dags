{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'result_id',
    full_refresh = False,
    tags = ["pre_processing"]
  )
}}

with empty_table as (
    select
        null as result_id,
        null as invocation_id,
        null as unique_id,
        null as database_name,
        null as schema_name,
        null as name,
        null as resource_type,
        null as status,
        cast(null as timestamp) as execute_started_at,
        cast(null as timestamp) as execute_completed_at,
        cast(null as float) as execution_time,
        cast(null as int) as rows_affected
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0