
with cte_detail_base as (
    select '1' as record_type_code
    , 'F' as record_action
    , left(coalesce(cast(j.job_id as varchar), '') || repeat(' ', 8), 8) as job_code
    , left(coalesce(j.job_description, '') || repeat(' ', 50), 50) as job_description
    , ' ' as customer_indicator
    , repeat(' ', 6) as role_code
    , f.event_time
    , p.deleted
    , p.position_id
    from {{ ref('fact_position') }} f
    inner join {{ ref('dim_position') }} p on f.position_key = p.position_key 
    inner join {{ ref('dim_job') }} j on f.job_key = j.job_key 
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    where p.position_id <> -1
    and j.job_id <> '-1'
    qualify row_number() over(partition by p.position_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day  

), cte_detail as (
    select *
    from cte_detail_base
    qualify row_number() over(partition by position_id order by event_time desc) = 1       -- most recent day   
    
), cte_header as (
    select '0' as record_type_code
    , to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS') as header_time_stamp
    , to_varchar(current_timestamp(), 'YYYYMMDD') as processing_date
    , 'BNZ' as financial_institution_id
    , 'HR' as data_source_system_code
    , left('obh005' || repeat(' ', 8), 8) as program_name                                   -- input
    , left('Employment Position Type File' || repeat(' ', 40), 40) as file_description      -- input
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
    , left('obh005' || repeat(' ', 8), 8) as program_name                                   -- input
    , left('Employment Position Type File' || repeat(' ', 40), 40) as file_description      -- input
    , '+' as sign1
    , right(repeat('0', 9) || '1' , 9) as file_identifier_code
    , '+' as sign2
    , right(repeat('0', 9) || to_varchar((select count(*) from cte_detail where coalesce(deleted, false) = false )), 9)  as record_count

), cte_union as (
    select record_type_code || header_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_header
    union 
    select record_type_code || record_action || job_code || job_description || customer_indicator || role_code as extract_column
    from cte_detail
    where coalesce(deleted, false) = false 
    union
    select record_type_code || trailer_time_stamp || processing_date || financial_institution_id || data_source_system_code || program_name || file_description || sign1 || file_identifier_code || sign2 || record_count as extract_column
    from cte_trailer

)
select *
from cte_union
order by 1
