import os
import configparser
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.lists.list import List
import pandas as pd
import datetime
import snowflake.connector

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# Build the path to the config.ini file
config_path = os.path.join(script_dir, 'config.ini')

# Snowflake Connection Credentials
config = configparser.ConfigParser()
config.read(config_path)

def connect_to_sharepoint():
    # Sharepoint Connection
    username = config.get("windows", "user")
    password = config.get("windows", "password")
    site_url = "https://quintiles.sharepoint.com/sites/Direct_to_Patient-Marketing_Operations"

    ctx_auth = AuthenticationContext(url=site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print("Connected to SharePoint site: {0}".format(web.properties['Title']))
        return ctx
    else:
        print(ctx_auth.get_last_error())
        return None

def connect_to_snowflake():
    # Snowflake connection
    ctx = snowflake.connector.connect(
        user=config.get("snowflake", "user"),
        password=config.get("snowflake", "password"),
        account=config.get("snowflake", "account"),
        warehouse=config.get("snowflake", "warehouse"),
        schema=config.get("snowflake", "schema"),
        role=config.get("snowflake", "role"))

    cursor = ctx.cursor()
    return cursor

def retrieve_list_data(ctx, list_name):
    list_obj = ctx.web.lists.get_by_title(list_name)
    items = list_obj.get_items().execute_query()
    data = [item.properties for item in items]
    return pd.DataFrame(data)

def retrieve_ytd_projects(cursor):
    ytd_query = """
    SELECT protocol 
    FROM PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAILS 
    WHERE ref_date >= '2024-01-01'
    GROUP BY 1
    """
    cursor.execute(ytd_query)
    return [row[0] for row in cursor.fetchall()]

def get_active_results(cursor, ytd_projects):
    actives_query = f"""
    WITH costs AS (
        SELECT protocol, SUM(value) AS costs 
        FROM PRDB_PROD.PROD_US9_PAR_RPR.V_DTP_MEDIA_COSTS 
        GROUP BY protocol
    )
    SELECT A.protocol,
        costs,
        MIN(ref_date) start_date,
        SUM(referrals) referrals,
        SUM(first_offcie_visit_scheduled) first_office_visit_scheduled,
        SUM(first_office_visit) first_office_visit,
        SUM(consented) consents,
        SUM(enrolled_randomized_ap) enrolled_randomized_ap
    FROM PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAIL_COUNTS A
    LEFT JOIN costs B ON A.protocol = B.protocol
    WHERE A.protocol IN ({','.join([f"'{p}'" for p in ytd_projects])})
    GROUP BY a.protocol, costs
    """
    cursor.execute(actives_query)
    active_results = cursor.fetchall()
    return pd.DataFrame(active_results, columns=['Protocol', 'Costs', 'Start Date', 'Referrals', 'FOVs Scheduled', 'FOVs', 'Consents', 'Enrolled Randomized AP'])

def get_dtp_study_results(cursor, ytd_projects):
    dtp_study_query = f"""
    WITH full_service_study AS (
        SELECT 
            protocol,
            COUNT(CASE WHEN active_randomized IS NOT NULL THEN patient_id ELSE NULL END) AS study_rands
        FROM PRDB_PROD.PROD_US9_PAR_RPR.TMDH_PRTCPNT_CVR_REF
        GROUP BY protocol
    ),
    standalone AS ( 
        SELECT 
            protcl_nbr protocol, 
            COUNT(CASE 
                WHEN prtcpnt_stat_nm IN ('Randomized') THEN e_cd 
                WHEN rndmised_dt IS NOT NULL THEN e_cd 
                ELSE NULL END) AS study_rands
        FROM PRDB_PROD.PROD_US9_PAR_RPR.V_IRT_PRTCPNT
        GROUP BY protcl_nbr
    ),
    study_rands AS (
        SELECT * FROM full_service_study
        UNION 
        SELECT * FROM standalone
    ),
    dtp_rands AS (
        SELECT protocol, SUM(enrolled_randomized_ap) dtp_rands 
        FROM PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAIL_COUNTS 
        GROUP BY protocol
    )
    SELECT dtp_rands.protocol, study_rands, dtp_rands, dtp_rands/study_rands dtp_prop 
    FROM dtp_rands 
    JOIN study_rands ON dtp_rands.protocol = study_rands.protocol
    WHERE dtp_rands.protocol IN ({','.join([f"'{p}'" for p in ytd_projects])})
    """
    cursor.execute(dtp_study_query)
    return pd.DataFrame(cursor.fetchall(), columns=['Protocol', 'Study Rands', 'DTP Rands', 'DTP Proportion'])

def merge_data(projects, active_results_df, dtp_study_results_df):
    merged_df = projects.merge(active_results_df, on='Protocol', how='left')
    merged_df = merged_df.merge(dtp_study_results_df, on='Protocol', how='left')
    
    result_df = merged_df[['Sponsor', 'Active', 'Title', 'Protocol', 'Duration', 'Target_x0023_Referrals', 'Target_x0023_FOVs', 'Target_x0023_Consents', 'Target_x0023_Rands', 'Start Date', 'Costs', 'Referrals', 'FOVs Scheduled', 'FOVs', 'Consents', 'Enrolled Randomized AP', 'Study Rands', 'DTP Rands', 'DTP Proportion']]
    result_df = result_df.rename(columns={'Target_x0023_Referrals': 'Target Referrals', 'Target_x0023_FOVs': 'Target FOVs', 'Target_x0023_Consents': 'Target Consents', 'Target_x0023_Rands': 'Target Rands'})
    return result_df

def calculate_metrics(result_df):
    result_df['Target FOV Rate'] = (result_df['Target FOVs'] / result_df['Target Referrals']).round(4)
    result_df['Actual FOV Rate'] = (result_df['FOVs'] / result_df['Referrals']).round(4)
    result_df['Costs'] = result_df['Costs'].astype(float)
    
    current_date = pd.Timestamp.now().normalize()
    result_df['Start Date'] = pd.to_datetime(result_df['Start Date'])
    result_df['elapsed 0'] = ((current_date - result_df['Start Date']).dt.days / 7) / result_df['Duration']
    result_df['elapsed 14'] = ((current_date - pd.DateOffset(days=14)) - result_df['Start Date']).dt.days / 7 / result_df['Duration']
    result_df['elapsed 30'] = ((current_date - pd.DateOffset(days=30)) - result_df['Start Date']).dt.days / 7 / result_df['Duration']
    result_df['elapsed 60'] = ((current_date - pd.DateOffset(days=60)) - result_df['Start Date']).dt.days / 7 / result_df['Duration']
    
    result_df['Target Referrals to Date'] = result_df['elapsed 0'] * result_df['Target Referrals']
    result_df['Target FOVs to Date'] = result_df['elapsed 14'] * result_df['Target FOVs']
    result_df['Target Consents to Date'] = result_df['elapsed 30'] * result_df['Target Consents']
    result_df['Target RANDs to Date'] = result_df['elapsed 60'] * result_df['Target Rands']

    result_df['Monthly Goal Refs'] = result_df['Referrals'] >= 0.9 * result_df['Target Referrals to Date'] 
    result_df['Monthly Goal Fovs'] = result_df['FOVs'] >= 0.9 * result_df['Target FOVs to Date']
    result_df['Monthly Goal Cons'] = result_df['Consents'] >= 0.9 * result_df['Target Consents to Date']
    result_df['Monthly Goal RANDs'] = result_df['Enrolled Randomized AP'] >= 0.9 * result_df['Target RANDs to Date']

    result_df = result_df[['Active', 'Sponsor', 'Title', 'Costs', 'Target FOV Rate', 'Actual FOV Rate', 'Monthly Goal Refs', 'Monthly Goal Fovs', 'Monthly Goal Cons', 'Monthly Goal RANDs', 'Study Rands', 'DTP Rands']]
    return result_df

def calculate_fov_targets(result_df):
    total_costs = result_df['Costs'].sum()
    result_df['Costs Proportion'] = (result_df['Costs'] / total_costs).round(4)
    
    result_df['% to Target'] = (result_df['Actual FOV Rate'] / result_df['Target FOV Rate']).round(4)
    result_df['% to Target'] = result_df['% to Target'].clip(upper=1.0)

    weighted_avg = (result_df['% to Target'] * result_df['Costs Proportion']).sum()
    
    fov_targets = result_df[['Title', 'Costs', 'Costs Proportion', 'Target FOV Rate', 'Actual FOV Rate', '% to Target']]
    fov_targets['Weighted Avg'] = weighted_avg
    return fov_targets

def main():
    sharepoint_context = connect_to_sharepoint()
    snowflake_cursor = connect_to_snowflake()

    list_name = "Direct to Patient Project Details"
    projects = retrieve_list_data(sharepoint_context, list_name)

    ytd_projects = retrieve_ytd_projects(snowflake_cursor)
    ytd_projects_df = pd.DataFrame({'Protocol': ytd_projects})
    projects['Protocol'] = projects['Protocol'].str.strip()
    ytd_merged_df = projects.merge(ytd_projects_df, on='Protocol', how='inner')
    ytd_projects = ytd_merged_df['Protocol'].tolist()

    active_results_df = get_active_results(snowflake_cursor, ytd_projects)
    dtp_study_results_df = get_dtp_study_results(snowflake_cursor, ytd_projects)

    result_df = merge_data(projects, active_results_df, dtp_study_results_df)
    result_df = calculate_metrics(result_df[result_df['Protocol'].isin(ytd_projects)])

    result_df.to_csv('C:/Users/q1032269/OneDrive - IQVIA/Documents/Gitz/Monthly Scorecard/scorecard_results.csv', index=False)

    fov_targets = calculate_fov_targets(result_df)
    fov_targets.to_csv('C:/Users/q1032269/OneDrive - IQVIA/Documents/Gitz/Monthly Scorecard/FOV_targets.csv', index=False)

if __name__ == "__main__":
    main()
