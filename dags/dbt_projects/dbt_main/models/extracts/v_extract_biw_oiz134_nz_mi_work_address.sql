with cte_fact_first_pass as (
    select f.*, c.colleague_id, c.deleted
    from {{ ref('fact_colleague') }} f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
    inner join {{ ref('dim_date') }} d on d.date_key = f.date_key
    inner join {{ ref('dim_time') }} t on t.time_key = f.time_key
    qualify row_number() over(partition by c.colleague_id, d.date order by t.time_of_day desc) = 1      -- most recent fact for each day  

), cte_fact_second_pass as (
    select *
    from cte_fact_first_pass
    qualify row_number() over(partition by colleague_id order by event_time desc) = 1           -- most recent day  

), cte_final as (
    select c.colleague_id as "Pers.no."
    , c.full_name as "Personnel Number"
    , o.organisation_unit_id as "Org.unit"
    , o.organisation_unit_description as "Organizational Unit"
    , case when wl.location_id = '-1' then cast(null as varchar) else wl.location_id end as "Building"
    , case when wl.location_id = '-1' then cast(null as varchar) else wl.address_line1 end as "Address supplement (c_o)"
    , wl.address_line2 as "House number and street"
    , wl.address_line3 as "Street"
    , wl.city as "Location"
    , wl.postal_code as "Post.code"
    , wl.region as "Rg"
    , case when pl.location_id = '-1' then cast(null as varchar) else pl.location_id end as "OU Building"
    , case when pl.location_id = '-1' then cast(null as varchar) else pl.address_line1 end as "OU Address supplement (c_o)"
    , pl.address_line2 as "OU House number and street"
    , pl.address_line3 as "OU Street"
    , pl.city as "OU Location"
    , pl.postal_code as "OU Post.code"
    , coalesce(pl.region, '') || '\t' as "OU Rg"                                        -- NB. Adding a TAB to mimic the behaviour of the original files.
    from cte_fact_second_pass f
    inner join {{ ref('dim_colleague') }} c on f.colleague_key = c.colleague_key
    inner join {{ ref('dim_location') }} wl on f.work_location_key = wl.location_key
    inner join {{ ref('dim_location') }} pl on f.position_location_key = pl.location_key
    inner join {{ ref('dim_organisation_unit') }} o on f.organisation_unit_key = o.organisation_unit_key
    where coalesce(f.deleted, false) = false 
    and c.colleague_id <> -1
)
select * 
from cte_final c
where c."Building" is not null or c."OU Building" is not null