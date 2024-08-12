{% macro generate_flatten_avro(model_name, source_name, json_column) %}
 
{% set get_json_path %}
 
    with cte_final as (
        select value
        , field_new
        , seq
        , field_type
        , parent
        --, field_sort
        , substring(SYS_CONNECT_BY_PATH(value, '_'), 2) as attribute_name
        , substring(SYS_CONNECT_BY_PATH(value, '.'), 2) as attribute_path
        from {{model_name}}       
        start with parent is null
        connect by parent = prior seq
        order by seq 

    )
    select attribute_path, attribute_name, seq
    from cte_final
    where field_type <> 'record'


{% endset %}
 
{# /* the value in the column will be the attributes of you exploded result  */ #}
{% set res = run_query(get_json_path) %}
{% if execute %}
    {% set res_list = res.columns[0].values() %}
{% else %}
    {% set res_list = [] %}
{% endif %}
 
{# /* explode JSON columns and format the column names  */ #}
select
{% for attribute_path in res_list | sort(attribute='seq') %}
    case when record_content:metadata.type = 'REMOVED' then 
        {{ json_column }}:{{ attribute_path }} 
    else
        {{ json_column }}:{{ attribute_path | replace("stateBefore", "stateAfter") }} 
    end as {{ attribute_path | replace("stateBefore.", "") | replace(".", "_")  }}{{ ", " if not loop.last else "" }}

{% endfor %}
 
from {{ source_name }}
 
{% endmacro %}
