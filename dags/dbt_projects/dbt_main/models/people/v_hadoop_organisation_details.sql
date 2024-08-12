{{ config(
  tags = ["main_processing"]
) }}

with cte_org_unit as (
    select cast(ou.organisation_unit_id as integer) as organisational_unit_nbr
    , ou.organisation_unit_description as organisational_unit_name
    , cast(ou.organisation_unit_level as integer) as level_nbr
    , cast(ou.organisation_unit_level1_id as integer) as level1_organisational_unit_nbr
    , ou.organisation_unit_level1_description as level1_organisational_unit_name
    , cast(ou.organisation_unit_level2_id as integer) as level2_organisational_unit_nbr
    , ou.organisation_unit_level2_description as level2_organisational_unit_name
    , cast(ou.organisation_unit_level3_id as integer) as level3_organisational_unit_nbr
    , ou.organisation_unit_level3_description as level3_organisational_unit_name
    , cast(ou.organisation_unit_level4_id as integer) as level4_organisational_unit_nbr
    , ou.organisation_unit_level4_description as level4_organisational_unit_name
    , cast(ou.organisation_unit_level5_id as integer) as level5_organisational_unit_nbr
    , ou.organisation_unit_level5_description as level5_organisational_unit_name
    , cast(ou.organisation_unit_level6_id as integer) as level6_organisational_unit_nbr
    , ou.organisation_unit_level6_description as level6_organisational_unit_name
    , cast(ou.organisation_unit_level7_id as integer) as level7_organisational_unit_nbr
    , ou.organisation_unit_level7_description as level7_organisational_unit_name
    , cast(ou.organisation_unit_level8_id as integer) as level8_organisational_unit_nbr
    , ou.organisation_unit_level8_description as level8_organisational_unit_name
    , cast(ou.organisation_unit_level9_id as integer) as level9_organisational_unit_nbr
    , ou.organisation_unit_level9_description as level9_organisational_unit_name
    , cast(ou.organisation_unit_level10_id as integer) as level10_organisational_unit_nbr
    , ou.organisation_unit_level10_description as level10_organisational_unit_name
    , f.event_time
    , ou.deleted
    from {{ ref('fact_organisation_unit') }} f
    inner join {{ ref('dim_organisation_unit') }} ou on f.organisation_unit_key = ou.organisation_unit_key
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    --where ou.organisation_unit_id  = 55900671
    qualify row_number() over(partition by ou.organisation_unit_id, d.date order by t.time_of_day desc) = 1
    
), cte_final as (
    select *
    from cte_org_unit
    qualify row_number() over(partition by organisational_unit_nbr order by event_time desc) = 1

)
select organisational_unit_nbr
, organisational_unit_name
, level_nbr
, level1_organisational_unit_nbr
, level1_organisational_unit_name
, level2_organisational_unit_nbr
, level2_organisational_unit_name
, level3_organisational_unit_nbr
, level3_organisational_unit_name
, level4_organisational_unit_nbr
, level4_organisational_unit_name
, level5_organisational_unit_nbr
, level5_organisational_unit_name
, level6_organisational_unit_nbr
, level6_organisational_unit_name
, level7_organisational_unit_nbr
, level7_organisational_unit_name
, level8_organisational_unit_nbr
, level8_organisational_unit_name
, level9_organisational_unit_nbr
, level9_organisational_unit_name
, level10_organisational_unit_nbr
, level10_organisational_unit_name
from cte_final
where coalesce(deleted, false) = false 