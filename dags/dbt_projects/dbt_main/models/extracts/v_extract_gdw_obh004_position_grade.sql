
with cte_detail_base as (
    select '1' as record_type_code
    , 'F' as record_action
    , left('BNZ' || repeat(' ', 10), 10) as financial_institution_id
    , left('SAP' || repeat(' ', 10), 10) as source_application_code
    , left(cast(p.position_id as varchar) || repeat(' ', 10), 10) as position_code
    , repeat(' ', 20) as filler
    , repeat('0', 12) as global_id
    , left(coalesce(to_char(p.position_start_date, 'YYYYMMDD'),'') || repeat(' ', 8), 8) as start_date
    , repeat('NV', 2) as ps_pay_grade_type
    , repeat('NV', 2) as ps_pay_grade_area
    , left('NV' || repeat(' ', 8), 8) as position_grade
    , left(replace(coalesce(p.job_level, ''), 'JL', '') || repeat(' ', 6), 6) as position_level
    , f.event_time
    , p.deleted
    from {{ ref('fact_position') }} f
    inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    where p.position_id <> -1
    qualify row_number() over(partition by p.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day  

), cte_detail as (
    select *
    from cte_detail_base
    qualify row_number() over(partition by position_code order by event_time desc) = 1       -- most recent day   
    
), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh004' || repeat(' ', 8), 8) as program_name                                   -- input
    , left('Position Grade File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh004' || repeat(' ', 8), 8) as program_name                                   -- input
    , left('Position Grade File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , right(repeat('0', 9) || '1' , 9) as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail where coalesce(deleted, false) = false)), 9)  as record_count

) , cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union 
    select record_type_code || record_action || financial_institution_id || source_application_code || position_code || filler || global_id || start_date || ps_pay_grade_type || ps_pay_grade_area || position_grade || position_level as extract_column
    from cte_detail
    where coalesce(deleted, false) = false  
    union
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer

)
select *
from cte_union
order by 1