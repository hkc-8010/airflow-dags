{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'view_id',
    full_refresh = False,
    tags = ["post_processing"],
    merge_exclude_columns = ['created_at', 'extract_subfolder', 'extract_file_format_name', 'extract_compression_override', 'extract_filename', 'extract_filename_add_date', 'extract_file_extension', 'generate_extract_flag', 'generate_enabled_timestamp', 'generate_enabled_by']
  )
}}

select MD5(table_schema || table_name || 'people_to_' || lower(split_part(table_name, '_', 3)) ) as view_id
, table_schema as view_schema
, table_name as view_name
, 'people_to_' || lower(split_part(table_name, '_', 3)) as extract_subfolder
, cast(null as varchar) as extract_file_format_name
, cast(null as varchar) as extract_compression_override
, cast(null as varchar) as extract_filename
, cast(null as boolean) as extract_filename_add_date
, cast(null as varchar) as extract_file_extension
, cast(null as boolean) as extract_add_header
, cast(null as boolean) as generate_extract_flag
, cast(null as timestamp) as generate_enabled_timestamp
, cast(null as varchar) as generate_enabled_by
, current_timestamp() as created_at
, current_timestamp() as modified_at
, '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
from information_schema.views
where table_schema = '{{ target.schema }}' 
and upper(table_name) like 'V_EXTRACT%'

{% if is_incremental() %}

and last_altered > (select max(modified_at) from {{ this }} )

{% endif %}


