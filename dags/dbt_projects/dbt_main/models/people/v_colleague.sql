{{ config(
  sql_header="set input_date = current_date();",
  tags = ["main_processing"]
) }}

select 
{{ get_relation_columns("dim_colleague", "c", "coll", true) }}
{{ get_relation_columns("dim_colleague", "m", "manager", true) }}
--{{ get_relation_columns("dim_position", "mp", "manager_pos", true) }}
{{ get_relation_columns("dim_country_group", "cg", "coll", true) }}
{{ get_relation_columns("dim_holiday_zone", "hz", "coll", true) }}
{{ get_relation_columns("dim_organisation_unit", "o", "org", true) }}
{{ get_relation_columns("dim_company_code", "cc", "org", true) }}
{{ get_relation_columns("dim_cost_center", "cst", "org", true) }}
{{ get_relation_columns("dim_contact", "ct", "org", true) }}
{{ get_relation_columns("dim_location", "l", "org", true) }}
{{ get_relation_columns("dim_position", "p", "coll_pos", true) }}
--{{ get_relation_columns("dim_organisation_unit", "po", "coll_pos_org", true) }}
--{{ get_relation_columns("dim_holiday_zone", "phz", "coll_pos", true) }}
--{{ get_relation_columns("dim_contact", "pct", "coll_pos", true) }}
--{{ get_relation_columns("dim_location", "pl", "coll_pos", true) }}
--{{ get_relation_columns("dim_cost_center", "pcst", "coll_pos", true) }}
{{ get_relation_columns("dim_job", "pj", "coll_pos", true) }}
{{ get_relation_columns("dim_contact", "ct2", "coll_contact", true) }}
{{ get_relation_columns("dim_location", "l2", "coll_location", true) }}
--mp.position_id as manager_position_id,
--mp.position_description as manager_position_decsription,
f.event_id,
f.event_time 
from {{ ref('fact_colleague') }} f
inner join {{ ref('dim_date') }}  d on d.date_key = f.date_key
inner join {{ ref('dim_time') }}  t on t.time_key = f.time_key
inner join {{ ref('dim_colleague') }}  c on f.colleague_key = c.colleague_key
inner join {{ ref('dim_colleague') }}  m on f.manager_key = m.colleague_key
--inner join {{ ref('dim_position') }}  mp on f.manager_position_key = mp.position_key
inner join {{ ref('dim_country_group') }}  cg on f.country_group_key = cg.country_group_key
inner join {{ ref('dim_holiday_zone') }}  hz on f.holiday_zone_key = hz.holiday_zone_key
inner join {{ ref('dim_organisation_unit') }}  o on f.organisation_unit_key = o.organisation_unit_key
inner join {{ ref('dim_company_code') }}  cc on f.organisation_unit_company_code_key = cc.company_code_key
inner join {{ ref('dim_cost_center') }}  cst on f.organisation_unit_cost_center_key = cst.cost_center_key
inner join {{ ref('dim_contact') }}  ct on f.organisation_unit_contact_key = ct.contact_key
inner join {{ ref('dim_location') }}  l on f.organisation_unit_location_key = l.location_key
inner join {{ ref('dim_position') }}  p on f.position_key = p.position_key
inner join {{ ref('dim_organisation_unit') }}  po on f.position_organisation_unit_key = po.organisation_unit_key
inner join {{ ref('dim_holiday_zone') }}  phz on f.position_holiday_zone_key = phz.holiday_zone_key
inner join {{ ref('dim_contact') }}  pct on f.position_contact_key = pct.contact_key
inner join {{ ref('dim_location') }}  pl on f.position_location_key = pl.location_key
inner join {{ ref('dim_cost_center') }}  pcst on f.position_cost_center_key = pcst.cost_center_key
inner join {{ ref('dim_job') }}  pj on f.position_job_key = pj.job_key
inner join {{ ref('dim_contact') }}  ct2 on f.work_contact_key = ct2.contact_key
inner join {{ ref('dim_location') }}  l2 on f.home_location_key = l2.location_key
where cast(f.event_time as date) = $input_date