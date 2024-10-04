-- Common Table Expressions (CTEs)
WITH rh_referral_details AS (
    SELECT
        -- Referral Hub Participant enrollment fields
        prtcpnt_e.refrl_id, 
        prtcpnt_e.prtcpnt_nm, 
        prtcpnt_e.pat_id AS sg_uid, 
        prtcpnt_e.scrning_id,
        CASE 
            WHEN LEFT(scrning_id, 3) = 'SH9' THEN 'AROAPOC3-3009'
            WHEN LEFT(scrning_id, 3) = 'SH3' THEN 'AROAPOC3-3003'
            WHEN LEFT(scrning_id, 3) = 'SH4' THEN 'AROAPOC3-3004'
            ELSE prtcpnt_e.stdy_id 
        END AS protocol,
        DATE(prtcpnt_e.ref_dt) AS ref_date, 
        DATE(DATEADD(DAY, -DAYOFWEEK(prtcpnt_e.ref_dt), DATEADD(DAY, 8, prtcpnt_e.ref_dt))) AS week_ends_sun, 
        prtcpnt_e.cmpn_mkting_attribution_id AS atid, 
        prtcpnt_e.utm_src_cd AS utm_source,
        prtcpnt_e.utm_medium_cd AS utm_medium, 
        prtcpnt_e.utm_cmpn_cd AS utm_campaign, 
        prtcpnt_e.utm_cntnt_cd AS utm_content, 
        prtcpnt_e.utm_term_cd AS utm_term, 
        prtcpnt_e.utmz_cd AS utm_z, 
        prtcpnt_e.prtcpnt_stat_cd AS participant_status, 
        prtcpnt_e.src_sys_stat_updt_desc AS last_status_change_notes, 
        prtcpnt_e.non_enrl_rsn_cd, 
        prtcpnt_e.prtcpnt_stat_last_updt_dt AS last_status_change_date,
        prtcpnt_e.clncl_stdy_site_init_vst_sched_dt, 
        prtcpnt_e.clncl_stdy_site_init_vst_dt AS initial_visit_occurred_date, 
        prtcpnt_e.prtcpnt_informed_consnt_dt AS informed_consent_date,
        prtcpnt_e.prtcpnt_enrl_dt AS enrolled_date, 
        prtcpnt_e.hcp_prtcpnt_rndmized_dt AS rh_randomized_date, 
        AS_DATE(tmdh.active_randomized) AS active_randomized,
        
        -- Referral Hub Participant Fields
        prtcpnt.gender_cd, 
        
        -- Race
        CASE 
            WHEN tmdh.race IS NOT NULL THEN INITCAP(tmdh.race)
            WHEN REGEXP_COUNT(REGEXP_REPLACE(prtcpnt.ethncy_cd, '1- Hispanic/Latino;', ''), ';') > 0 THEN 'Two or more Races'
            ELSE REGEXP_REPLACE(REGEXP_REPLACE(prtcpnt.ethncy_cd, '[\\d\\s-]+', ''), 'Hispanic/Latino;', '')
        END AS race,
        
        -- Ethnicity 
        CASE 
            WHEN tmdh.ethnicity IS NOT NULL THEN INITCAP(tmdh.ethnicity)
            WHEN REGEXP_LIKE(prtcpnt.ethncy_cd, '1. Hispanic/Latino.*') THEN 'Hispanic/Latino'
            ELSE NULL 
        END AS ethnicity,
        
        -- Age split into buckets
        CASE
            WHEN prtcpnt.age < 18 THEN '< 18'
            WHEN prtcpnt.age < 25 THEN '18 - 24'
            WHEN prtcpnt.age < 35 THEN '25 - 34'
            WHEN prtcpnt.age < 45 THEN '35 - 44'
            WHEN prtcpnt.age < 55 THEN '45 - 54'
            WHEN prtcpnt.age < 65 THEN '55 - 64'
            ELSE '65+' 
        END AS age,
        
        -- Referral Hub Site Fields 
        site.stdy_site_nm AS stdy_site_name, 
        site.stdy_site_nbr_extrnl_id AS site_number, 
        site.site_billg_ctry_nm AS site_country, 
        site.site_billg_ctry_cd AS site_country_code, 
        site.site_billg_city_nm AS site_city, 
        site.site_billg_st_nm AS state, 
        site.site_billg_pstl_cd AS site_zip,
        
        -- WMT Category
        rcrd.rec_typ_nm AS Category,
        
        -- WMT Campaign Details
        CASE 
            WHEN cmpn.CMPN_MKTING_CHNL_NM = 'Facebook Ads' THEN 'META'
            ELSE cmpn.CMPN_MKTING_CHNL_NM 
        END AS channel,
        cmpn.ad_nm AS ad_name, 
        cmpn.ad_grp_nm AS ad_group_name, 
        cmpn.ad_body_copy_txt AS ad_body_copy_text, 
        cmpn.ad_chnl_cmpn_nm AS channel_campaign_name, 
        cmpn.ad_display_url_txt AS display_url, 
        cmpn.ad_headln_txt AS headline_text, 
        cmpn.cmpn_scrner_flow_nm AS screener_flow, 
        cmpn.cmpn_scrner_snstvty_nm AS screener_sensitivity,
        cmpn.alloc_model_nm AS cost_allocation_model, 
        
        -- WMT Vendor
        vndr.acct_nm AS vendor, 
        
        -- Various status flags
        CASE 
            WHEN prtcpnt_e.clncl_stdy_site_init_vst_sched_dt IS NOT NULL 
                OR prtcpnt_e.clncl_stdy_site_init_vst_sched_ind = 1 
                OR clncl_stdy_site_init_vst_dt IS NOT NULL 
                OR clncl_stdy_site_init_vst_ind
                OR prtcpnt_e.prtcpnt_informed_consnt_dt IS NOT NULL 
                OR prtcpnt_e.prtcpnt_informed_consnt_cd = 'true' 
                OR prtcpnt_e.prtcpnt_stat_cd NOT IN (
                    'Received',
                    'Pending Referral',
                    'Failed Referral',
                    'Excluded from Referring'
                )
                OR tmdh.current_participant_status IS NOT NULL
            THEN prtcpnt_e.prtcpnt_nm 
            ELSE NULL 
        END AS contact_attempted,
        
        CASE
            WHEN prtcpnt_e.prtcpnt_informed_consnt_dt IS NOT NULL
                OR prtcpnt_e.clncl_stdy_site_init_vst_sched_dt IS NOT NULL 
                OR prtcpnt_e.clncl_stdy_site_init_vst_sched_ind = 1 
                OR prtcpnt_e.prtcpnt_informed_consnt_cd = 'true'
                OR (prtcpnt_e.prtcpnt_stat_cd = 'Unable to Reach' AND hist.old_val_txt IS NOT NULL)
                OR prtcpnt_e.prtcpnt_stat_cd NOT IN (
                    'Received',
                    'Pending Referral',
                    'Failed Referral',
                    'Excluded from Referring',
                    'Contact Attempted',
                    'Unable to Reach'
                )
                OR tmdh.current_participant_status IS NOT NULL
            THEN prtcpnt_e.prtcpnt_nm 
            ELSE NULL 
        END AS contact_successful,
        
        -- Additional status flags (first_office_visit_scheduled, first_office_visit, consented, etc.)
        -- ... (implement similar CASE statements for each status flag)
        
        -- Historical status
        hist.old_val_txt AS post_contact_status_hist
        
    FROM PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT_ENRL prtcpnt_e
    LEFT JOIN (
        SELECT protocol, patient_id, current_participant_status, race, ethnicity, active_randomized, consent, screened, screen_failed, completed, dropped, rescreened
        FROM PRDB_PROD.PROD_US9_PAR_RPR.TMDH_PRTCPNT_CVR_REF 
        WHERE patient_id IS NOT NULL
    ) tmdh ON (
        REGEXP_REPLACE(prtcpnt_e.scrning_id, '-') = REGEXP_REPLACE(tmdh.patient_id, '-')
    ) AND prtcpnt_e.stdy_id = tmdh.protocol
    LEFT JOIN PRDB_PROD.PROD_US9_PAR_RPR.V_RH_CLNCL_STDY_SITE site ON site.clncl_stdy_site_id = prtcpnt_e.clncl_stdy_site_id
    LEFT JOIN PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT prtcpnt ON prtcpnt_e.prtcpnt_id = prtcpnt.prtcpnt_id
    LEFT JOIN (
        SELECT 
            prtcpnt_enrl_id,
            ARRAY_TO_STRING(ARRAY_AGG(old_val_txt), ',') AS old_val_txt 
        FROM PRDB_PROD.PROD_US9_PAR_RPR.V_RH_PRTCPNT_ENRL_HIST
        WHERE chg_fld_nm = 'Participant_Status__c' 
        AND old_val_txt NOT IN ('Received','Contact Attempt in Progress', 'Unable to Reach', 'Referral Accepted', 'Contact Attempted')
        GROUP BY prtcpnt_enrl_id
    ) hist ON hist.prtcpnt_enrl_id = prtcpnt_e.prtcpnt_enrl_id
    LEFT JOIN PRDB_PROD.PROD_US9_PAR_RPR.V_WMT_CMPN cmpn ON cmpn.cmpn_mkting_attribution_id = prtcpnt_e.cmpn_mkting_attribution_id
    LEFT JOIN PRDB_PROD.PROD_US9_PAR_RPR.V_WMT_REC_TYP rcrd ON rcrd.rec_typ_id = cmpn.cmpn_rec_typ_id
    LEFT JOIN PRDB_PROD.PROD_US9_PAR_RPR.V_WMT_ACCT vndr ON cmpn.vend_nm = vndr.acct_id
    WHERE 
        LOWER(site.stdy_site_nm) NOT LIKE '%iqvia test%' 
        AND (LOWER(prtcpnt_e.utm_src_cd) NOT LIKE '%test%' 
            OR LOWER(prtcpnt_e.utm_src_cd) NOT LIKE '%uat%'
            OR LOWER(prtcpnt_e.utm_src_cd) NOT IN ('eleven', 'six')
            OR prtcpnt_e.utm_src_cd IS NULL)
        AND refrl_src_typ_nm IN ('ePR_Campaign', 'ePR') 
        AND prtcpnt_e.stdy_id IN ('AROAPOC3-3004', 'AROAPOC3-3003', 'AROAPOC3-3009')
),

rh_referral_details_counts AS (
    SELECT 
        protocol,
        ref_date,
        week_ends_sun,
        utm_source,
        utm_medium,
        utm_z,
        clncl_stdy_site_init_vst_sched_dt,
        initial_visit_occurred_date,
        informed_consent_date,
        CASE 
            WHEN active_randomized IS NOT NULL THEN active_randomized
            WHEN enrolled_date IS NOT NULL THEN enrolled_date
            ELSE rh_randomized_date 
        END AS randomized_date,
        CASE 
            WHEN gender_cd NOT IN (1,2,3) AND race = 'Black or African American' THEN 1 
            WHEN gender_cd NOT IN ('1','2','3') AND race != 'Black or African American' THEN 2
            ELSE gender_cd 
        END AS gender_cd,
        CASE 
            WHEN race = 'Caucasian/White' THEN 'White'
            WHEN race = 'Prefernottosay' THEN 'Prefer not to say'
            WHEN race = 'Twoormoreraces' THEN 'Two or more races'
            ELSE race 
        END AS race, 
        CASE   
            WHEN ethnicity = 'Hispanic or Latino' THEN 'Hispanic/Latino'
            ELSE ethnicity 
        END AS ethnicity, 
        age,
        stdy_site_name,
        site_number,
        site_country,
        site_country_code,
        site_city,
        state,
        site_zip,
        Category,
        channel,
        vendor,
        COUNT(DISTINCT prtcpnt_nm) AS referrals,
        COUNT(DISTINCT contact_attempted) AS contact_attempted,
        COUNT(DISTINCT contact_successful) AS contact_successful,
        COUNT(DISTINCT first_office_visit_scheduled) AS first_offcie_visit_scheduled,
        COUNT(DISTINCT first_office_visit) AS first_office_visit,
        COUNT(DISTINCT consented) AS consented,
        COUNT(DISTINCT screening_complete_fa) AS screened_fa,
        COUNT(DISTINCT screening_complete_ap) AS screened_ap,
        COUNT(DISTINCT screen_failed_fa) AS screen_failed_fa,
        COUNT(DISTINCT screen_failed_ap) AS screen_failed_ap,
        COUNT(DISTINCT enrolled_randomized_fa) AS enrolled_randomized_fa,
        COUNT(DISTINCT enrolled_randomized_ap) AS enrolled_randomized_ap,
        COUNT(DISTINCT completed) AS completed,
        COUNT(DISTINCT dropped) AS dropped,
        COUNT(DISTINCT rescreened) AS rescreened,
        COUNT(DISTINCT unable_to_reach_no_contact) AS unable_to_reach_no_contact,
        COUNT(DISTINCT unable_to_reach_contacted) AS unable_to_reach_contacted,
        COUNT(DISTINCT not_eligible) AS not_eligible,
        COUNT(DISTINCT patient_declined) AS patient_declined,
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


