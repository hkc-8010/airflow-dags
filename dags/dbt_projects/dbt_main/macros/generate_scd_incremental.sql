

{% macro generate_scd_incremental(column_list, if_incremental) %}


    {# Create 2 lists from our input column_list: one for column selection and the other for hashing & comparison in conditional_change_event. Prefix z. so we can substitute in the SQL below. #}
    {%- set hash_column_list = [] -%}
    {%- set select_column_list = [] -%}
    {%- set unknown_column_list = [] -%}

    {%- for col in column_list -%}
        {%- do hash_column_list.append(
            "coalesce(cast(z." ~ col ~ " as string), '')"
        ) -%}
        {%- do select_column_list.append(
            "z." ~ col ~ ""
        ) -%}

        {%- if loop.index ==1 %}
            {%- do unknown_column_list.append(" '-1' as " ~ col ~ "") -%}
        {%- elif loop.index ==2 %}
            {%- do unknown_column_list.append(" 'Unknown' as " ~ col ~ "") -%}
        {%- else -%}
            {%- do unknown_column_list.append(" null as " ~ col ~ "") -%}
        {%- endif -%}

        {%- if not loop.last %}
            {%- do hash_column_list.append(" || '-' || ") -%}
            {%- do select_column_list.append(", ") -%}
            {%- do unknown_column_list.append(", ") -%}
        {%- endif -%}
    {%- endfor -%}
    
    -- standard cte's to identify our delta
    , source_rows_value_change as (
        -- identify records that have changed based on all columns in our list
        select *
        , md5(concat(coalesce(cast(id as string),''), coalesce(cast(to_timestamp(event_time) as string),''))) as dbt_scd_id
        , '{{ invocation_id }}' || '.' || '{{ model.unique_id }}' as dbt_model_run_id
        , event_time as dbt_valid_from
        , null as dbt_valid_to
        , conditional_change_event( {{ dbt.hash(hash_column_list | join('') | replace('z.','') ) }} ) over (partition by id order by event_time asc) value_change 
        from source_rows_base

    ), source_rows_first_value_change as (
        -- target the earliest instance of each change
        select *
        from source_rows_value_change
        qualify row_number() over(partition by id, value_change order by event_time asc ) = 1
    ), source_rows as (
        -- and only those records that are more recent than those in our destination
        select *
        from source_rows_first_value_change
        where 1=1
        {% if if_incremental %}
            and event_time > (select coalesce(max(event_time),'1900-01-01') from {{ this }})
        {% endif %}
    )


    {% if if_incremental %}
        , destination_rows as (
            -- get all open records from the destination
            select *
            from {{ this }} 
            where dbt_valid_to is null 
        )
        , new_valid_to as (
            -- identify those destination rows that can be closed off - because we have newer changes in the source
            select d.id
            , s.dbt_valid_from as dbt_valid_to
            from source_rows s
            join destination_rows d
            on s.id = d.id
            and s.dbt_scd_id != d.dbt_scd_id
            and {{ dbt.hash(hash_column_list | join('') | replace('z.','s.')) }} <> {{ dbt.hash(hash_column_list | join('') | replace('z.','d.')) }}     
        )
        , add_new_valid_to_existing as (
            -- add the source's dbt_valid_from as the destination's dbt_valid_to to close them off
            select {{ select_column_list | join('') | replace('z.','d.') }}
            , d.event_id
            , d.event_time
            , d.event_msg_source
            , d.created_at
            , d.dbt_valid_from
            , n.dbt_valid_to 
            , d.dbt_scd_id
            , d.dbt_model_run_id
            from destination_rows d
            left join new_valid_to n
            on d.id = n.id
            where n.dbt_valid_to is not null        -- null means no change so exclude to minimize db updates
        )
        , final as (
            -- repeat conditional_change_event to see if our next source record's value is the same as the existing open destination record. we will exclude if so.
            -- NB. to avoid duplicates between source & destination exclude created_at this stage
            select *
            , conditional_change_event( {{ dbt.hash(hash_column_list | join('') | replace('z.','')) }} ) over (partition by id order by event_time asc) value_change  
            from (select {{ select_column_list | join('') | replace('z.','n.') }}
                    , n.event_id
                    , n.event_time
                    , n.event_msg_source
                    , n.dbt_valid_from
                    , n.dbt_valid_to 
                    , n.dbt_scd_id
                    , n.dbt_model_run_id
                    from add_new_valid_to_existing n
                    union
                    select {{ select_column_list | join('') | replace('z.','s.') }} 
                    , s.event_id
                    , s.event_time
                    , s.event_msg_source
                    , s.dbt_valid_from
                    , lead(s.event_time, 1) over (partition by s.id order by s.event_time) as dbt_valid_to 
                    , s.dbt_scd_id
                    , s.dbt_model_run_id
                    from source_rows s
                ) x
        )
        select {{ select_column_list | join('') | replace('z.','f.') }}
        , f.event_id
        , f.event_time
        , f.event_msg_source
        , case when f.id = '-1' then to_timestamp('1900-01-01') else coalesce(a.created_at, current_timestamp()) end as created_at      -- could be from destination or new in source
        , null as modified_at
        , f.dbt_valid_from
        , f.dbt_valid_to
        , f.dbt_scd_id
        , f.dbt_model_run_id
        from final f
        left join add_new_valid_to_existing a on f.id = a.id and f.dbt_scd_id = a.dbt_scd_id
        qualify row_number() over(partition by f.id, f.value_change order by f.event_time asc ) = 1

    {% endif %}

    {% if not if_incremental %}
        select {{ select_column_list | join('') | replace('z.','s.') }} 
        , s.event_id
        , s.event_time
        , s.event_msg_source
        , current_timestamp() as created_at
        , null as modified_at
        , s.dbt_valid_from
        , lead(s.event_time, 1) over (partition by s.id order by s.event_time) as dbt_valid_to 
        , s.dbt_scd_id
        , s.dbt_model_run_id
        from source_rows s

        union
        
        select {{ unknown_column_list | join('') }} 
        , '0'
        , to_timestamp('1900-01-01')
        , 'dbt'
        , to_timestamp('1900-01-01')
        , null
        , to_timestamp('1900-01-01')
        , null
        , {{ var("unknown_hash") }}
        , '{{ invocation_id }}' || '.' || '{{ model.unique_id }}'

    {% endif %}

{% endmacro %}
