--sql
with rh_referral_details as (
select
--Referral Hub Participant enrl fields
prtcpnt_e.refrl_id, prtcpnt_e.prtcpnt_nm, prtcpnt_e.pat_id as sg_uid, prtcpnt_e.scrning_id as scrning_id,
case 
    when left(scrning_id, 3) = 'SH9' then 'AROAPOC3-3009'
    when left(scrning_id, 3) = 'SH3' then 'AROAPOC3-3003'
    when left(scrning_id, 3) = 'SH4' then 'AROAPOC3-3004'
    else prtcpnt_e.stdy_id end as protocol,
date(prtcpnt_e.ref_dt) as ref_date, date(dateadd(day,-DAYOFWEEK(prtcpnt_e.ref_dt) ,DATEADD(day,8,prtcpnt_e.ref_dt))) as week_ends_sun, prtcpnt_e.cmpn_mkting_attribution_id as atid, prtcpnt_e.utm_src_cd as utm_source,
prtcpnt_e.utm_medium_cd as utm_medium, prtcpnt_e.utm_cmpn_cd as utm_campaign, prtcpnt_e.utm_cntnt_cd as utm_content, prtcpnt_e.utm_term_cd as utm_term, prtcpnt_e.utmz_cd as utm_z, 
prtcpnt_e.prtcpnt_stat_cd as participant_status, prtcpnt_e.src_sys_stat_updt_desc as last_status_change_notes, prtcpnt_e.non_enrl_rsn_cd, prtcpnt_e.prtcpnt_stat_last_updt_dt as last_status_change_date,
prtcpnt_e.clncl_stdy_site_init_vst_sched_dt, prtcpnt_e.clncl_stdy_site_init_vst_dt as initial_visit_occurred_date, prtcpnt_e.prtcpnt_informed_consnt_dt as informed_consent_date,
prtcpnt_e.prtcpnt_enrl_dt as enrolled_date, prtcpnt_e.hcp_prtcpnt_rndmized_dt as rh_randomized_date, as_date(tmdh.active_randomized) as active_randomized,
--Referral Hub Participant Fields
prtcpnt.gender_cd, 
--race
case 
    when tmdh.race is not null then INITCAP(tmdh.race)
    when REGEXP_COUNT
        (
           REGEXP_REPLACE
            (
                prtcpnt.ethncy_cd, '1- Hispanic/Latino;', ''
            ), ';'
        ) > 0 then TO_VARCHAR('Two or more Races')
    else regexp_replace(regexp_replace(prtcpnt.ethncy_cd, '[\\d\\s-]+', ''), 'Hispanic/Latino;', '') end as race,
--ethnicity 
case 
    when tmdh.ethnicity is not null then INITCAP(tmdh.ethnicity)
    when regexp_like(prtcpnt.ethncy_cd, '1. Hispanic/Latino.*') then 'Hispanic/Latino'
    else null end as ethnicity,
--age split into buckets
case
    when prtcpnt.age < 18 then '< 18'
    when prtcpnt.age < 25 then '18 - 24'
    when prtcpnt.age < 35 then '25 - 34'
    when prtcpnt.age < 45 then '35 - 44'
    when prtcpnt.age < 55 then '45 - 54'
    when prtcpnt.age < 65 then '55 - 64'
    else '65+' end as age,
--Referral Hub Site Fields 
site.stdy_site_nm as stdy_site_name, site.stdy_site_nbr_extrnl_id as site_number, site.site_billg_ctry_nm as site_country, site.site_billg_ctry_cd as site_country_code, 
site.site_billg_city_nm as site_city, site.site_billg_st_nm as state, site.site_billg_pstl_cd as site_zip,
--WMT Category
rcrd.rec_typ_nm as Category,
--WMT Campaign Details
case 
    when cmpn.CMPN_MKTING_CHNL_NM = 'Facebook Ads' then 'META'
    else cmpn.CMPN_MKTING_CHNL_NM end as channel,
cmpn.ad_nm as ad_name, cmpn.ad_grp_nm as ad_group_name, 
cmpn.ad_body_copy_txt as ad_body_copy_text, cmpn.ad_chnl_cmpn_nm as channel_campaign_name, cmpn.ad_display_url_txt as display_url, 
cmpn.ad_headln_txt as headline_text, cmpn.cmpn_scrner_flow_nm as screener_flow, cmpn.cmpn_scrner_snstvty_nm as screener_sensitivity,
cmpn.alloc_model_nm as cost_allocation_model, 
--WMT Vendor
vndr.acct_nm as vendor, 
--Contact Attempted
case 
    when prtcpnt_e.clncl_stdy_site_init_vst_sched_dt is not null 
    or prtcpnt_e.clncl_stdy_site_init_vst_sched_ind = 1 
    or clncl_stdy_site_init_vst_dt is not null 
    or clncl_stdy_site_init_vst_ind
    or prtcpnt_e.prtcpnt_informed_consnt_dt is not null 
    or prtcpnt_e.prtcpnt_informed_consnt_cd = 'true' or 
    prtcpnt_e.prtcpnt_stat_cd not in (
        'Received',
        'Pending Referral',
        'Failed Referral',
        'Excluded from Referring'
        )
    or tmdh.current_participant_status is not null
    then prtcpnt_e.prtcpnt_nm else null end as contact_attempted,
--Contact Successful 
CASE
    when prtcpnt_e.prtcpnt_informed_consnt_dt is not null
    or prtcpnt_e.clncl_stdy_site_init_vst_sched_dt is not null 
    or prtcpnt_e.clncl_stdy_site_init_vst_sched_ind = 1 
    or prtcpnt_e.prtcpnt_informed_consnt_cd = 'true'
    or (prtcpnt_e.prtcpnt_stat_cd = 'Unable to Reach' and hist.old_val_txt is not null)
    or prtcpnt_e.prtcpnt_stat_cd not in (
        'Received',
        'Pending Referral',
        'Failed Referral',
        'Excluded from Referring',
        'Contact Attempted',
        'Unable to Reach'
        )
    or tmdh.current_participant_status is not null
    
    then prtcpnt_e.prtcpnt_nm else null end as contact_successful,
--first office visit scheduled
case
    when prtcpnt_e.clncl_stdy_site_init_vst_sched_dt is not null 
    or prtcpnt_e.clncl_stdy_site_init_vst_sched_ind = 1 
    or clncl_stdy_site_init_vst_dt is not null 
    or clncl_stdy_site_init_vst_ind
    or prtcpnt_e.prtcpnt_informed_consnt_dt is not null 
    or prtcpnt_e.prtcpnt_informed_consnt_cd = 'true' 
    or tmdh.current_participant_status is not null
    or prtcpnt_e.prtcpnt_stat_cd in (
        'Ready to Screen',        
        'Screening In Progress - Wash Out Period',
        'Screening In Progress',
        'Screening Complete',
        'Screening Passed',
        'Withdrew Consent After Screening',
        'In Wash Out Period',
        'Screening Failed',
        'Enrollment Failed',
        'Drop Out',
        'Enrollment Success',
        'Participation Complete',
        'Randomization Success',
        'Treatment Period Started',
        'Trial Complete'
        )
    then prtcpnt_e.prtcpnt_nm else null end as first_office_visit_scheduled,
--first office visit
case
    when prtcpnt_e.clncl_stdy_site_init_vst_dt is not null
    or prtcpnt_e.clncl_stdy_site_init_vst_ind = 1
    or prtcpnt_e.prtcpnt_informed_consnt_dt is not null
    or prtcpnt_e.prtcpnt_informed_consnt_cd = 'true'
    or tmdh.current_participant_status is not null
    or prtcpnt_e.prtcpnt_stat_cd in (
        'Screening In Progress - Wash Out Period',
        'Screening In Progress',
        'Screening Complete',
        'Screening Passed',
        'Withdrew Consent After Screening',
        'In Wash Out Period',
        'Screening Failed',
        'Enrollment Failed',
        'Drop Out',
        'Enrollment Success',
        'Participation Complete',
        'Randomization Success',
        'Treatment Period Started',
        'Trial Complete'
        )
    then prtcpnt_e.prtcpnt_nm else null end as first_office_visit,
--consented
case
    when prtcpnt_e.prtcpnt_informed_consnt_dt is not null
    or prtcpnt_e.prtcpnt_informed_consnt_cd = 'true'
    or tmdh.consent is not null
    or prtcpnt_e.prtcpnt_stat_cd in (
        'Screening In Progress - Wash Out Period',
        'Screening In Progress',
        'Screening Complete',
        'Screening Passed',
        'Withdrew Consent After Screening',
        'In Wash Out Period',
        'Screening Failed',
        'Enrollment Failed',
        'Drop Out',
        'Enrollment Success',
        'Participation Complete',
        'Randomization Success',
        'Treatment Period Started',
        'Trial Complete'
        )
    then prtcpnt_e.prtcpnt_nm else null end as consented,
--screened all potential
case
    when prtcpnt_e.prtcpnt_stat_cd in (
        'Screening Complete',
        'Screening Passed',
        'Withdrew Consent After Screening',
        'In Wash Out Period',
        'Screening Failed',
        'Enrollment Failed',
        'Drop Out',
        'Enrollment Success',
        'Participation Complete',
        'Randomization Success',
        'Treatment Period Started',
        'Trial Complete'
        )
    or tmdh.screened is not null
    then prtcpnt_e.prtcpnt_nm else null end as screening_complete_ap,
--screened fully authenticated
case
    when tmdh.screened is not null 
    then prtcpnt_e.prtcpnt_nm else null end as screening_complete_fa,
--screen failed ap
case 
    when prtcpnt_e.prtcpnt_stat_cd in ('Screening Failed', 'Enrollment Failed')
    or tmdh.screen_failed is not null
    then prtcpnt_e.prtcpnt_nm else null end as screen_failed_ap,
--screen failed fa
case 
    when prtcpnt_e.prtcpnt_stat_cd in ('Screening Failed', 'Enrollment Failed')
    and tmdh.screen_failed is not null
    then prtcpnt_e.prtcpnt_nm else null end as screen_failed_fa,
--enrolled_fa
CASE   
    when prtcpnt_e.prtcpnt_stat_cd in (
        'Enrollment Success',
        'Participation Complete',
        'Randomization Success',
        'Treatment Period Started',
        'Trial Complete',
        'Follow-Up Period Started',
        'Drop Out'
        )
    and tmdh.active_randomized is not null 
    and tmdh.current_participant_status not in ('Screen Failed','Screened')
    then prtcpnt_e.prtcpnt_nm else null end as enrolled_randomized_fa,
--enrolled_ap
case 
    when prtcpnt_e.prtcpnt_stat_cd in (
        'Enrollment Success',
        'Participation Complete',
        'Randomization Success',
        'Treatment Period Started',
        'Trial Complete',
        'Follow-Up Period Started'
        )
    or (tmdh.active_randomized is not null 
        and tmdh.current_participant_status not in ('Screen Failed','Screened'))
    then prtcpnt_e.prtcpnt_nm else null end as enrolled_randomized_ap,
--completed
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Trial Complete'
    or tmdh.completed is not null
    then prtcpnt_e.prtcpnt_nm else null end as completed,
--dropped
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Drop Out'
    or tmdh.dropped is not null
    then prtcpnt_e.prtcpnt_nm else null end as dropped,
--rescreened
case 
    when tmdh.rescreened is not null
    then prtcpnt_e.prtcpnt_nm else null end as rescreened,
--Unable to Reach: No Contact
hist.old_val_txt as post_contact_status_hist,
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Unable to Reach' 
    and hist.old_val_txt is null  
    then prtcpnt_e.prtcpnt_nm else null end as unable_to_reach_no_contact,
--Unable to Reach: Contacted
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Unable to Reach' 
    and hist.old_val_txt is not null  
    then prtcpnt_e.prtcpnt_nm else null end as unable_to_reach_contacted,
--Contacted - Not Suitable not Eligible
case 
    --when prtcpnt_e.prtcpnt_stat_cd in ('Contacted - Not Suitable','Pre-review Failed') 
    when prtcpnt_e.non_enrl_rsn_cd in (
        'Declined Practitioner', 
        'Did Not Meet Inclusion/Exclusion Criteria', 
        'Didn\'t Meet Pre-Screening Eligibility',
        'Failed Inclusion/Exclusion Criteria', 
        'PI Decision') OR
        prtcpnt_e.prtcpnt_stat_cd in ('Pre-review Failed')
    then prtcpnt_e.prtcpnt_nm else null end as not_eligible,
--Contacted - Not Suitable patient declined
case 
    --when prtcpnt_e.prtcpnt_stat_cd in ('Contacted - Not Suitable','Pre-review Failed') 
    when  prtcpnt_e.non_enrl_rsn_cd in (
        'Participant Not Interested', 
        'Patient Decision',
        'Patient Declined',
        'Protocol Concerns')
    then prtcpnt_e.prtcpnt_nm else null end as patient_declined,
--Childcare Issues
case 
    when prtcpnt_e.non_enrl_rsn_cd = 'Childcare Issues'
    then prtcpnt_e.prtcpnt_nm else null end as childcare_issues,
--Travel Issues
case 
    when prtcpnt_e.non_enrl_rsn_cd = 'Transportation Issues'
    then prtcpnt_e.prtcpnt_nm else null end as travel_issues,
-- No Show
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Participant No Show' or 
    prtcpnt_e.non_enrl_rsn_cd = 'Didn\'t Show for Initial Visit' OR
    (prtcpnt_e.clncl_stdy_site_init_vst_sched_dt <= current_date and prtcpnt_e.clncl_stdy_site_init_vst_dt is null)
    then prtcpnt_e.prtcpnt_nm else null end as status_no_show,
-- Active Funnel: Declined Consent
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Declined Consent'
    then prtcpnt_e.prtcpnt_nm else null end as status_declined_consent,
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Contacted - Not Suitable'
    and (prtcpnt_e.non_enrl_rsn_cd = 'Other' or prtcpnt_e.non_enrl_rsn_cd is null)
    then prtcpnt_e.prtcpnt_nm else null end contacted_not_suitable_other,
-- Active Funnel: No Contact Attempted
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Received' 
    then prtcpnt_e.prtcpnt_nm else null end as no_contact_attempted, 
-- Active Funnel: Contacted Attempted
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Contacted Attempted'
    then prtcpnt_e.prtcpnt_nm else null end as status_contacted_attempted,
-- Active Funnel: Contacted Not Scheduled
case 
    when prtcpnt_e.prtcpnt_stat_cd = 'Successfully Contacted' and prtcpnt_e.clncl_stdy_site_init_vst_sched_dt is null
    then prtcpnt_e.prtcpnt_nm else null end as status_contacted_not_scheduled
from PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT_ENRL prtcpnt_e
-- Join to modified TMDH table
left join 
    (
        select protocol, patient_id, current_participant_status, race, ethnicity, active_randomized, consent, screened, screen_failed, completed, dropped, rescreened
        from PRDB_PROD.PROD_US9_PAR_RPR.TMDH_PRTCPNT_CVR_REF where patient_id is not null
    ) tmdh on (
        REGEXP_REPLACE(prtcpnt_e.scrning_id, '-') =  REGEXP_REPLACE(tmdh.patient_id, '-')
        --or concat(substr(prtcpnt_e.scrning_id, 4,3), substr(prtcpnt_e.scrning_id,8,3)) = REGEXP_REPLACE(tmdh.patient_id, '-')
        ) 
    and prtcpnt_e.stdy_id = tmdh.protocol
--join to v_rh_clncl_stdy_site table
left join PRDB_PROD.PROD_US9_PAR_RPR.V_RH_CLNCL_STDY_SITE site on site.clncl_stdy_site_id = prtcpnt_e.clncl_stdy_site_id
left join PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT prtcpnt on prtcpnt_e.prtcpnt_id = prtcpnt.prtcpnt_id
--join Historical Status Table
left join (
        select 
        prtcpnt_enrl_id,ARRAY_TO_STRING(ARRAY_AGG(old_val_txt), ',') as old_val_txt 
        from PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT_ENRL_HIST
        where chg_fld_nm = 'Participant_Status__c' 
        and old_val_txt not in ('Received','Contact Attempt in Progress', 'Unable to Reach', 'Referral Accepted', 'Contact Attempted')
        group by prtcpnt_enrl_id) hist on hist.prtcpnt_enrl_id = prtcpnt_e.prtcpnt_enrl_id
--join WMT Campaign table
left join PRDB_PROD.PROD_US9_PAR_RPR.V_WMT_CMPN cmpn on cmpn.cmpn_mkting_attribution_id = prtcpnt_e.cmpn_mkting_attribution_id
--join WMT Record table
left join PRDB_PROD.PROD_US9_PAR_RPR.V_WMT_REC_TYP rcrd on rcrd.rec_typ_id = cmpn.cmpn_rec_typ_id
--join WMT Vendor
left join PRDB_PROD.PROD_US9_PAR_RPR.V_WMT_ACCT vndr on cmpn.vend_nm = vndr.acct_id
--Filters for IQVIA and Test/UAT
where 
    lower(site.stdy_site_nm) not like '%iqvia test%' 
    and (lower(prtcpnt_e.utm_src_cd) not like '%test%' 
        or lower(prtcpnt_e.utm_src_cd) not like '%uat%'
        or lower(prtcpnt_e.utm_src_cd) not in ('eleven', 'six')
        or prtcpnt_e.utm_src_cd is null)
and refrl_src_typ_nm in ('ePR_Campaign', 'ePR') 
and prtcpnt_e.stdy_id in ('AROAPOC3-3004', 'AROAPOC3-3003', 'AROAPOC3-3009')
),
rh_referral_details_counts as (
SELECT protocol
,ref_date
,week_ends_sun
, utm_source
,utm_medium,utm_z
,clncl_stdy_site_init_vst_sched_dt
,initial_visit_occurred_date
,informed_consent_date
,case 
    when active_randomized is not null then active_randomized
    when enrolled_date is not null then enrolled_date
    else rh_randomized_date end as randomized_date
, case 
    when gender_cd not in (1,2,3) and race = 'Black or African American' then 1 
    when gender_cd not in ('1','2','3') and race != 'Black or African American' then 2
    else gender_cd end as gender_cd 
,case 
    when race = 'Caucasian/White' then 'White'
    when race = 'Prefernottosay' then 'Prefer not to say'
    when race = 'Twoormoreraces' then 'Two or more races'
    else race end as race, 
CASE   
    when ethnicity = 'Hispanic or Latino' then 'Hispanic/Latino'
    else ethnicity end as ethnicity, 
age,
stdy_site_name
,site_number
,site_country
,site_country_code
,site_city,state
,site_zip
,Category
,channel
,vendor
,count(distinct prtcpnt_nm) as referrals
,count(distinct contact_attempted) as contact_attempted
,count(distinct contact_successful) as contact_successful
,count(distinct first_office_visit_scheduled) as first_offcie_visit_scheduled
,count(distinct first_office_visit) as first_office_visit
,count(distinct consented) as consented
,count(distinct screening_complete_fa) as screened_fa
,count(distinct screening_complete_ap) as screened_ap
,count(distinct screen_failed_fa) as screen_failed_fa
,count(distinct screen_failed_ap) as screen_failed_ap
,count(distinct enrolled_randomized_fa) as enrolled_randomized_fa
,count(distinct enrolled_randomized_ap) as enrolled_randomized_ap
,count(distinct completed) as completed,count(distinct dropped) as dropped
,count(distinct rescreened) as rescreened
,count(distinct unable_to_reach_no_contact) as unable_to_reach_no_contact
,count(distinct unable_to_reach_contacted) as unable_to_reach_contacted
,count(distinct not_eligible) as not_eligible
,count(distinct patient_declined) as patient_declined
,count(distinct childcare_issues) as childcare_issues
,count(distinct travel_issues) as travel_issues
,count(distinct status_no_show) as no_show
,count(distinct contacted_not_suitable_other) as other
,count(distinct no_contact_attempted) as no_contact_attempted
,count(distinct status_contacted_attempted) as status_contacted_attempted
,count(distinct status_contacted_not_scheduled) AS status_contacted_not_scheduled
,count(distinct status_declined_consent) AS declined_consent
from rh_referral_details
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)

select PROTOCOL, REF_DATE, WEEK_ENDS_SUN, UTM_SOURCE, UTM_MEDIUM, UTM_Z, CLNCL_STDY_SITE_INIT_VST_SCHED_DT, INITIAL_VISIT_OCCURRED_DATE, INFORMED_CONSENT_DATE, RANDOMIZED_DATE, GENDER_CD, 
AGE, STDY_SITE_NAME, SITE_NUMBER, SITE_COUNTRY, SITE_COUNTRY_CODE, SITE_CITY, STATE, SITE_ZIP, CATEGORY, CHANNEL, VENDOR, REFERRALS, CONTACT_ATTEMPTED, CONTACT_SUCCESSFUL, 
FIRST_OFFCIE_VISIT_SCHEDULED, FIRST_OFFICE_VISIT, CONSENTED, SCREENED_FA, SCREENED_AP, SCREEN_FAILED_FA, SCREEN_FAILED_AP, ENROLLED_RANDOMIZED_FA, ENROLLED_RANDOMIZED_AP, 
COMPLETED, DROPPED, RESCREENED, UNABLE_TO_REACH_NO_CONTACT, UNABLE_TO_REACH_CONTACTED, NOT_ELIGIBLE, PATIENT_DECLINED, CHILDCARE_ISSUES, TRAVEL_ISSUES, NO_SHOW, OTHER, 
NO_CONTACT_ATTEMPTED, STATUS_CONTACTED_ATTEMPTED, STATUS_CONTACTED_NOT_SCHEDULED, DECLINED_CONSENT,
case when ethnicity = 'Hispanic Or Latino' then 'Hispanic/Latino' 
when ethnicity is null then 'Not Hispanic Or Latino' else ethnicity end as ETHNICITY,
case when race = 'Twoormoreraces' then 'Two or more races' else race end as race
from rh_referral_detail_counts
where protocol in ('AROAPOC3-3004', 'AROAPOC3-3003', 'AROAPOC3-3009')


