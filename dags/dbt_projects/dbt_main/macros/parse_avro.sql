
{% macro parse_avro(avro_file) %}

    -- NB. dependencies: UDF py_Flatten; RAW s3 bucket
    with cte_base as (
        -- derive a join key by removing identifiers
        select key
        , coalesce(nullif(replace(replace(replace(replace(replace(case when split_part(key, '.', -1) in('name', 'type', 'default') then 
                                                                    substring(key, 1, len(key) - len(split_part(key, '.', -1)) -1 )
                                                                else
                                                                    substring(key, 1, len(key) - len(split_part(key, '.', -1)) - len(split_part(key, '.', -2)) -2 )     -- fields.0.type.fields.4.type.symbols.0
                                                                end, 'type.0001.name', ''), '.type.0001', ''),     '.type', ''), 'False.', ''), '.items', '')                          
                                                                , ''), key) as field
        , cast(value as string) as value
        , case when key like '%type.0001.name' then 1 end as complex_type
        , row_number() over(order by key) as rowid
        from table(flatten(input => parse_json(py_Flatten(select parse_json($1) from @people_raw_stage/{{avro_file}} (file_format => 'PEOPLE_JSON_FORMAT')))   )) f 
        where value not in('stateAfter')        -- we're only interested in one image
        and key <> 'False.name'                 -- name of our top-level node. Eg. DataEventValue

    ), cte_base_join as (
        -- build out our name, type, default & nested type values
        select n.key, n.field, n.value, n.rowid, t.value as field_type, d.value as field_default, t1.value as field_type_nested, n.complex_type 
        from cte_base n
        left join (select key, field, value from cte_base where key like '%.type') t on n.field = t.field
        left join (select key, field, value from cte_base where key like '%.type.0001') t1 on n.field = t1.field
        left join (select key, field, value from cte_base where key like '%.default') d on n.field = d.field
        where n.key like '%.name' and n.key not like '%.type.name'

    ), cte_base_join_inheritee as (
        -- get list of nested types that we need to back-fill from equivalents detailed earlier in the schema
        -- eg. ..."type": ["null", "Contact"],..."Contact" can be inherited from an earlier definition  
        select *
        from cte_base_join
        where coalesce(field_type_nested, '') not in('string', 'number', 'integer', 'object', 'array', 'boolean', 'int', 'enum', '')     -- ignore JSON basic types

    ), cte_base_join_inheritee_map as (
        -- map the inheritee's nested types to our base (inheritor)
        select i.field as inheritee_field, 'record' as inheritee_field_type, b.field as inhertitable_field, i.value
        from cte_base_join_inheritee i
        left join cte_base_join b on b.value = i.field_type_nested 

    ), cte_back_filled as (
        -- back-fill attributes for the inheritee
        select key, field, field as field_new, value, rowid, field_type, field_default, field_type_nested, 0 as back_filled, complex_type 
        from cte_base_join 
        union all 
        select b.key, b.field, replace(b.field, i.inhertitable_field, i.inheritee_field) as field_new, case when b.field_type = 'record' then i.value else b.value end, b.rowid, b.field_type, b.field_default, b.field_type_nested, 1 as back_filled, b.complex_type 
        from cte_base_join b
        inner join cte_base_join_inheritee_map i on b.field like i.inhertitable_field || '%'

    ), cte_back_filled_final as(
        -- group fields and exclude records we're not interested in
        select bf.key, bf.field, bf.field_new, bf.value, bf.rowid, coalesce(bf.field_type, bf.field_type_nested) as field_type, bf.back_filled 
        , split_part(bf.field_new, '.', -2) || '.' || split_part(bf.field_new, '.', -1) as last_field
        , array_to_string(array_slice(split(bf.field_new,'.'),0,-2),'.') as up_to_last_field
        , row_number() over(partition by left(bf.field_new, 11) order by field_new, rowid) as group_seq
        , row_number() over(order by field_new, rowid) as seq
        --, cast(replace(replace(bf.field_new,'fields.', ''), '.', '') as integer) as field_sort
        from cte_back_filled bf
        where 1=1
        and bf.complex_type is null                 -- we've back-filled so now exclude the complex types/inheritor records
        and coalesce(bf.field_type, bf.field_type_nested) in('string', 'number', 'integer', 'object', 'array', 'boolean', 'int', 'enum', '', 'record') 
        order by field_new, rowid
        
    ), cte_final as (
        -- get parent
        select f.*
            , case when f.group_seq = 1 then 
                null 
            else 
                case when f.field_type = 'record' then 
                    f1.seq 
                else
                    (select max(a.seq) from cte_back_filled_final a where a.field_type = 'record' and a.seq < f.seq and a.up_to_last_field <> f.up_to_last_field)   
                end 
            end as parent
        from cte_back_filled_final f
        left join cte_back_filled_final f1 on f.up_to_last_field = f1.field_new 
        order by f.seq 
            
    )
    select f.*
    , current_timestamp() as created_at
    , current_timestamp() as modified_at
    , '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
    from cte_final f
    order by seq

{% endmacro %}