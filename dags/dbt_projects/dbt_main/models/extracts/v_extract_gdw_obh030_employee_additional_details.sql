with cte_per_are as (
    -- TODO company_code_id needs to be mapped back to SAP equivalent...perhaps changes on group/subgroup too.
    -- TODO CNTCT, ACTCDE, REACDE to do when confidential comes through
    select x.*
    from (select c.colleague_id, c.hire_date, cc.company_code_id, cc.company_code_description
            , f.event_time, c.deleted
            from {{ ref('fact_colleague') }} f
            inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key 
            inner join {{ ref('dim_company_code') }} cc on f.organisation_unit_company_code_key = cc.company_code_key
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where c.colleague_id <> -1
            and cc.company_code_id <> '-1'
            qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
        ) x
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1       -- most recent day      

), cte_per_sub as (
    select x.*
    from (select c.colleague_id, c.hire_date, hz.holiday_zone_id, hz.holiday_zone_description 
            , f.event_time, c.deleted
            from {{ ref('fact_colleague') }} f
            inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key 
            inner join {{ ref('dim_holiday_zone') }} hz on f.holiday_zone_key = hz.holiday_zone_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where c.colleague_id <> -1
            and hz.holiday_zone_id <> '-1'
            qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
        ) x
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1       -- most recent day   

)
, cte_cus_spe as (
    select x.*
    from (select c.colleague_id, p.colleague_position_start_date, case when c.status = 'ACTIVE' then '0' else '1' end cus_spec_code
            , case when c.status = 'ACTIVE' then 'Operational' else 'Non-Operational' end cus_spec_description
            , f.event_time, c.deleted
            from {{ ref('fact_colleague') }} f
            inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key 
            inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where c.colleague_id <> -1
            and p.position_id <> -1
            qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
        ) x
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1       -- most recent day   

), cte_detail as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as company_code
    , left('SAP' || repeat(' ', 10), 10) as source_file
    , left(cast(x.colleague_number as varchar) || coalesce(to_char(x.hire_date, 'YYYYMMDD'),'') || repeat(' ', 30), 30) as employee_number
    , left(x.classification_code || repeat(' ', 6), 6) as classification_code               -- TBC
    , left(x.code || repeat(' ', 6), 6) as value                                            -- TBC
    , left(coalesce(to_char(x.hire_date, 'YYYYMMDD'), '') || repeat(' ', 8), 8) as start_date
    , left(x.description || repeat(' ', 255), 255) as description
    from (select 'PerAre' as classification_code, colleague_id as colleague_number, hire_date, company_code_id as code, company_code_description as description
            from cte_per_are
            where coalesce(deleted, false) = false  
            union
            select 'PerSub' as classification_code, colleague_id as colleague_number, hire_date, holiday_zone_id, holiday_zone_description
            from cte_per_sub
            where coalesce(deleted, false) = false  
            union
            select 'CusSpe' as classification_code, colleague_id as colleague_number, colleague_position_start_date, cus_spec_code, cus_spec_description
            from cte_cus_spe
            where coalesce(deleted, false) = false
        ) x

), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh030' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee Additional Details File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , '000000000' record_count

), cte_trailer as (
    select '9' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as trailer_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh030' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Employee Additional Details File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || company_code || source_file || employee_number || classification_code || value || start_date || description as extract_column
    from cte_detail
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1
