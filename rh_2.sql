-- CTE for referral details
WITH rh_referral_details AS (
    -- ... (keep the existing rh_referral_details CTE as is)
),

-- CTE for aggregated counts
rh_counts AS (
    SELECT
        protocol,
        ref_date,
        week_ends_sun,
        utm_source,
        utm_medium,
        utm_z,
        high_risk_ind,
        clncl_stdy_site_init_vst_sched_dt,
        initial_visit_occurred_date,
        informed_consent_date,
        CASE 
            WHEN active_randomized IS NOT NULL THEN active_randomized
            WHEN enrolled_date IS NOT NULL THEN enrolled_date
            ELSE rh_randomized_date
        END AS randomized_date,
        gender_cd,
        CASE 
            WHEN race = 'Caucasian/White' THEN 'White'
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
        COUNT(DISTINCT first_office_visit_scheduled) AS first_office_visit_scheduled,
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
        COUNT(DISTINCT childcare_issues) AS childcare_issues,
        COUNT(DISTINCT travel_issues) AS travel_issues,
        COUNT(DISTINCT status_no_show) AS no_show,
        COUNT(DISTINCT contacted_not_suitable_other) AS other,
        COUNT(DISTINCT no_contact_attempted) AS no_contact_attempted,
        COUNT(DISTINCT status_contacted_attempted) AS status_contacted_attempted,
        COUNT(DISTINCT status_contacted_not_scheduled) AS status_contacted_not_scheduled,
        COUNT(DISTINCT status_declined_consent) AS declined_consent,
        AVG(days_unprocessed) AS average_days_unprocessed
    FROM rh_referral_details
    GROUP BY
        protocol, ref_date, week_ends_sun, utm_source, utm_medium, utm_z, high_risk_ind,
        clncl_stdy_site_init_vst_sched_dt, initial_visit_occurred_date, informed_consent_date,
        gender_cd, race, ethnicity, age, stdy_site_name, site_number, site_country,
        site_country_code, site_city, state, site_zip, Category, channel, vendor,
        CASE 
            WHEN active_randomized IS NOT NULL THEN active_randomized
            WHEN enrolled_date IS NOT NULL THEN enrolled_date
            ELSE rh_randomized_date
        END
)

-- Main query
SELECT *
FROM rh_counts
WHERE protocol IN ('M23-784', 'M23-703');
