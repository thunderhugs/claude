    with rh_counts as (
        select 
            protocol,
            case 
                when site_country_code = 'GB' then 'UK' 
                else site_country_code end as country,
            channel, 
            min(ref_date) as min_ref_dt,
            max(ref_date) as max_ref_dt, 
            datediff(month, min(ref_date),max(ref_date)) as months_active,
            count(distinct site_number) as no_sites, 
            sum(referrals) as referrals, 
            sum(first_offcie_visit_scheduled) as sched_fov,
            sum(first_office_visit) as fovs, 
            sum(consented) as consents, sum(screened_ap) as screened,
            sum(enrolled_randomized_ap) as rands
    from PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAIL_COUNTS
    group by 1,2,3
    ),
    tmdh as (
            select 
            protcl_nbr,
            pri_thptc_area_nm as therapy_area,
            clncl_trial_phase_nm as phase,
            clinstdy_pri_indctn_nm as primary_indication
            from PRDB_PROD.PROD_US9_PAR_RPR.V_TMDH_CLINSTDY
            group by 1,2,3,4
    ),
    costs as (
            select 
            protocol,
            channel,
            case 
                when country = 'UK' 
                then 'GB' else country end as country,
            sum(value) as costs
            from PRDB_PROD.PROD_US9_PAR_RPR.DTP_COSTS_ADJUSTED
            group by 1,2,3
            )

select 
    *, 
    sum(c.costs) over (partition by c.country) /  
    sum(a.referrals) over (partition by a.country) as Country_CPR 
from rh_counts a 
left join tmdh b on a.protocol = b.protcl_nbr 
left join costs c on a.protocol = c.protocol and a.country = c.country and a.channel = c.channel
where c.costs is not null;