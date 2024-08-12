with cte_fact_first_pass as (
    select f.*, c.colleague_id, p.position_id 
    from {{ ref('fact_position') }} f
    inner join {{ ref('dim_position') }} p on f.position_key = p.position_key
    inner join {{ ref('dim_colleague') }} c on f.current_incumbent_key = c.colleague_key
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    qualify row_number() over(partition by p.position_id, c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day  

), cte_fact_second_pass as (
    select *
    from cte_fact_first_pass
    qualify row_number() over(partition by position_id, colleague_id order by event_time desc) = 1           -- most recent day  

)
select c.colleague_id as "Pers.no."
, c.legal_last_name as "Last name"
, c.legal_first_name as "First name"
, cc.gender_identity as "Gender"                         
, p.position_id as "Position"
, p.position_description as "PositionDesc"                  -- NB. spec says Position as column name resulting in duplicate
, o.organisation_unit_id as "Org.unit"
, o.organisation_unit_description as "Organizational Unit"
, j.job_id as "Job"
, j.job_description as "JobDesc"                            -- NB. spec says Job as column name resulting in duplicate
, cst.cost_center_id  as "Cost ctr"                      
, cst.cost_center_Description as "CostCenter"                    
, cast(null as varchar)  as "EE Group"                      -- TBC contractual
, cast(null as varchar)  as "EE Gp Name"                    -- TBC contractual
, cast(null as varchar)  as "EE Subgrp"                     -- TBC contractual
, cast(null as varchar)  as "EE SG Name"                    -- TBC contractual
, cast(null as varchar) as "WklyHrOcc"                      -- TBC
, cast(null as varchar) as "StdWklyHrs"                     -- TBC
, cast(null as varchar) as "Mth_ActHrs"                     -- TBC
, cast(null as varchar) as "Mth_StdHrs"                     -- TBC
, '' || '\t' as "FTE"                                       -- TBC  NB. Adding a TAB to mimic the behaviour of the original files.
from cte_fact_second_pass f
inner join {{ ref('dim_colleague') }} c on f.current_incumbent_key = c.colleague_key
inner join {{ ref('dim_position') }} p on f.position_key = p.position_key
inner join {{ ref('dim_job') }} j on f.job_key = j.job_key
inner join {{ ref('dim_organisation_unit') }} o on f.organisation_unit_key = o.organisation_unit_key
inner join {{ ref('dim_cost_center') }} cst on f.cost_center_key = cst.cost_center_key
inner join {{ ref('dim_colleague_confidential') }} cc on f.current_incumbent_confidential_key = cc.colleague_confidential_key
where coalesce(c.deleted, false) = false 
and coalesce(p.deleted, false) = false 
and p.position_id  <> -1 
and c.colleague_id  <> -1