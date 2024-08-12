with cte_rptpos as (
    select x.*
    from (select p.position_id, p.position_description, cast(mp.position_id as varchar) as manager_position_id, mp.position_description as manager_position_description, p.deleted
            , f.event_time
            from {{ ref('fact_colleague') }} f
            inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
            inner join {{ ref('dim_position') }} mp on f.manager_position_key = mp.position_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where mp.position_id <> -1
            and p.position_id <> -1
            qualify row_number() over(partition by p.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
        ) x
    qualify row_number() over(partition by position_id order by event_time desc) = 1       -- most recent day      

), cte_posorg as (
    select x.*
    from (select p.position_id, p.position_description, cast(ou.organisation_unit_id as varchar) as organisation_unit_id, ou.organisation_unit_description, p.deleted, p.chief_position_flag 
            , f.event_time
            from {{ ref('fact_position') }} f
            inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
            inner join {{ ref('dim_organisation_unit') }} ou on f.organisation_unit_key = ou.organisation_unit_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where ou.organisation_unit_id <> -1
            and p.position_id <> -1
            qualify row_number() over(partition by p.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
    ) x
    qualify row_number() over(partition by position_id order by event_time desc) = 1       -- most recent day 

), cte_ep as (
    select x.*
    from (select p.position_id, p.position_description, cc.cost_center_id, cc.cost_center_description, p.deleted
            , f.event_time
            from {{ ref('fact_position') }} f
            inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
            inner join {{ ref('dim_cost_center') }} cc on f.cost_center_key = cc.cost_center_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where cc.cost_center_id <> '-1'
            and p.position_id <> -1
            qualify row_number() over(partition by p.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
    ) x
    qualify row_number() over(partition by position_id order by event_time desc) = 1       -- most recent day 

), cte_orgorg as (
    select x.*
    from (select ou.organisation_unit_id, ou.organisation_unit_description, ou.deleted, cast(coalesce(organisation_unit_level9_id, organisation_unit_level8_id, organisation_unit_level7_id, organisation_unit_level6_id, organisation_unit_level5_id, organisation_unit_level4_id, organisation_unit_level3_id, organisation_unit_level2_id, organisation_unit_level1_id, parent_organisation_unit_id) as varchar) as max_level_organisation_unit_id
            , f.event_time
            from {{ ref('fact_organisation_unit') }} f
            inner join {{ ref('dim_organisation_unit') }} ou on f.organisation_unit_key = ou.organisation_unit_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where ou.organisation_unit_id <> -1
            qualify row_number() over(partition by ou.organisation_unit_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
    ) x
    qualify row_number() over(partition by organisation_unit_id order by event_time desc) = 1       -- most recent day 

), cte_hroucc as (
    select x.*
    from (select ou.organisation_unit_id, ou.organisation_unit_description, cc.cost_center_id, cc.cost_center_description, ou.deleted
            , f.event_time
            from {{ ref('fact_organisation_unit') }} f
            inner join {{ ref('dim_organisation_unit') }} ou on f.organisation_unit_key = ou.organisation_unit_key 
            inner join {{ ref('dim_cost_center') }} cc on f.cost_center_key = cc.cost_center_key 
            inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
            inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
            where cc.cost_center_id <> '-1'
            and ou.organisation_unit_id <> -1
            qualify row_number() over(partition by ou.organisation_unit_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day
    ) x
    qualify row_number() over(partition by organisation_unit_id order by event_time desc) = 1       -- most recent day 

), cte_detail as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(x.superordinate as varchar) || repeat(' ', 10), 10) as superior_org_unit
    , repeat(' ', 20) as filler
    , repeat('0', 12) as global_id
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id1                       -- TBC
    , left('SAP' || repeat(' ', 10), 10) as source_application_code1                        -- TBC
    , left(cast(x.subordinate as varchar) || repeat(' ', 10), 10) as subordinate_org_unit
    , repeat(' ', 20) as filler1
    , repeat('0', 12) as global_id1   
    , left(x.hierarchy_code || repeat(' ', 6), 6) as hierarchy_code
    , repeat(' ', 50) as filler2
    from (select 'RPTPOS' as hierarchy_code, position_id as subordinate, manager_position_id as superordinate
            from cte_rptpos
            where coalesce(deleted, false) = false  
            union
            select 'POSN' as hierarchy_code, position_id as subordinate, manager_position_id as superordinate
            from cte_rptpos
            where coalesce(deleted, false) = false  
            union
            select 'POSORG' as hierarchy_code, position_id as subordinate, organisation_unit_id as superordinate
            from cte_posorg
            where coalesce(deleted, false) = false  
            union
            select 'EP' as hierarchy_code, position_id as subordinate, cost_center_id as superordinate
            from cte_ep
            where coalesce(deleted, false) = false  
            union
            select 'RELCHF' as hierarchy_code, position_id as subordinate, organisation_unit_id as superordinate
            from cte_posorg
            where coalesce(deleted, false) = false  
            and coalesce(chief_position_flag, false) = true
            union
            select 'ORGORG' as hierarchy_code, organisation_unit_id as subordinate, max_level_organisation_unit_id as superordinate
            from cte_orgorg
            where coalesce(deleted, false) = false  
            union
            select 'HROUCC' as hierarchy_code, organisation_unit_id as subordinate, cost_center_id as superordinate
            from cte_hroucc
            where coalesce(deleted, false) = false  
        ) x

), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh003' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Org Unit Relationship File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh003' || repeat(' ', 8), 8) as program_name                       -- input
    , left('Org Unit Relationship File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , '000000001' as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail)), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union all
    select record_type_code || record_action || financial_institution_id || source_application_code || superior_org_unit || filler || global_id || financial_institution_id1 || source_application_code1 || subordinate_org_unit || filler1 || global_id1 || hierarchy_code || filler2  as extract_column
    from cte_detail
    union all
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer
)
select *
from cte_union
order by 1
