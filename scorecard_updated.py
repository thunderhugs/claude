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
        user = config.get("snowflake", "user"),
        password = config.get("snowflake", "password"),
        account= config.get("snowflake", "account"),
        warehouse = config.get("snowflake", "warehouse"),
        schema = config.get("snowflake", "schema"),
        role = config.get("snowflake", "role"))

    cursor = ctx.cursor()
    return cursor

# Use the functions
sharepoint_context = connect_to_sharepoint()
snowflake_cursor = connect_to_snowflake()

def retrieve_list_data(ctx, list_name):
    list_obj = ctx.web.lists.get_by_title(list_name)
    items = list_obj.get_items().execute_query()
    data = []
    for item in items:
        data.append(item.properties)
    return pd.DataFrame(data)

# Retrieve data from the SharePoint list
list_name = "Direct to Patient Project Details"
projects = retrieve_list_data(sharepoint_context, list_name)

def retrieve_ytd_projects(cursor):
    ytd_query = """
    SELECT protocol 
    FROM PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAILS 
    WHERE ref_date >= '2024-01-01'
    GROUP BY 1
    """
    cursor.execute(ytd_query)
    return [row[0] for row in cursor.fetchall()]

# Retrieve the Year to Date projects
ytd_projects = retrieve_ytd_projects(snowflake_cursor)

print(ytd_projects)

# Inner join ytd_projects with projects
ytd_projects_df = pd.DataFrame({'Protocol': ytd_projects})
projects['Protocol'] = projects['Protocol'].str.strip()  # Strip trailing spaces
ytd_merged_df = projects.merge(ytd_projects_df, on='Protocol', how='inner')
ytd_projects = ytd_merged_df['Protocol'].tolist()

print('-----------------------------------')
print(ytd_projects)

# Custom SQL query using the active projects
actives_query = f"""
with costs as (
    select protocol, sum(value) as costs 
    from PRDB_PROD.PROD_US9_PAR_RPR.V_DTP_MEDIA_COSTS 
    group by protocol
)

select A.protocol,
    costs,
    min(ref_date) start_date,
    sum(referrals) referrals,
    sum(first_offcie_visit_scheduled) first_office_visit_scheduled,
    sum(first_office_visit) first_office_visit,
    sum(consented) consents,
    sum(enrolled_randomized_ap) enrolled_randomized_ap
from PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAIL_COUNTS A
left join costs B on A.protocol = B.protocol
where A.protocol in ({','.join([f"'{p}'" for p in ytd_projects])})
group by a.protocol, costs"""

# Integrate the query using the active_projects list to filter
dtp_study = f"""
with full_service_study as (
select 
protocol,
count(case when active_randomized is not null then patient_id else null end) as study_rands,
from PRDB_PROD.PROD_US9_PAR_RPR.TMDH_PRTCPNT_CVR_REF
group by protocol),
standalone as ( 
select 
protcl_nbr protocol, 
count(case 
    when prtcpnt_stat_nm in ('Randomized') then e_cd 
    when rndmised_dt is not null then e_cd 
    else null end) as study_rands
from PRDB_PROD.PROD_US9_PAR_RPR.V_IRT_PRTCPNT
group by protcl_nbr),
study_rands as (
select * from full_service_study
union 
select * from standalone),
dtp_rands as (
select protocol, sum(enrolled_randomized_ap) dtp_rands from PRDB_PROD.PROD_US9_PAR_RPR.RH_REFERRAL_DETAIL_COUNTS group by protocol)

select dtp_rands.protocol, study_rands, dtp_rands, dtp_rands/study_rands dtp_prop from dtp_rands 
join study_rands on dtp_rands.protocol = study_rands.protocol
where dtp_rands.protocol in ({','.join([f"'{p}'" for p in ytd_projects])})
"""

# Execute the query
snowflake_cursor.execute(dtp_study)

# Fetch all the results
results = snowflake_cursor.fetchall()

# Convert results to a dataframe
results_df_2 = pd.DataFrame(results, columns=['Protocol', 'Study Rands', 'DTP Rands', 'DTP Proportion'])

# Execute the query
snowflake_cursor.execute(actives_query)

# Fetch all the results
active_results = snowflake_cursor.fetchall()

# Convert active_results to a dataframe
active_results_df = pd.DataFrame(active_results, columns=['Protocol', 'Costs', 'Start Date', 'Referrals', 'FOVs Scheduled', 'FOVs', 'Consents', 'Enrolled Randomized AP'])

# Join projects, active_results_df, and results_df_2 on Protocol column
merged_df = projects.merge(active_results_df, on='Protocol', how='left')
merged_df = merged_df.merge(results_df_2, on='Protocol', how='left')

# Select the desired columns and rename them
result_df = merged_df[['Sponsor', 'Active', 'Title', 'Protocol', 'Duration', 'Target_x0023_Referrals', 'Target_x0023_FOVs', 'Target_x0023_Consents', 'Target_x0023_Rands', 'Start Date', 'Costs', 'Referrals', 'FOVs Scheduled', 'FOVs', 'Consents', 'Enrolled Randomized AP', 'Study Rands', 'DTP Rands', 'DTP Proportion']]
result_df = result_df.rename(columns={'Target_x0023_Referrals': 'Target Referrals', 'Target_x0023_FOVs': 'Target FOVs', 'Target_x0023_Consents': 'Target Consents', 'Target_x0023_Rands': 'Target Rands'})

# Calculate the Target FOV Rate
result_df['Target FOV Rate'] = (result_df['Target FOVs'] / result_df['Target Referrals']).round(4)

#Calculate the Actual FOV Rate
result_df['Actual FOV Rate'] = (result_df['FOVs'] / result_df['Referrals']).round(4)

#Convert Costs to float
result_df['Costs'] = result_df['Costs'].astype(float)

# Filter to only include active projects listed in ytd_projects
result_df = result_df[result_df['Protocol'].isin(ytd_projects)]

# Calculate the current date
current_date = pd.Timestamp.now().normalize()

# Convert 'Start Date' to datetime
result_df['Start Date'] = pd.to_datetime(result_df['Start Date'])

# Calculate the % elapsed
result_df['elapsed 0'] = ((current_date - result_df['Start Date']).dt.days / 7) / result_df['Duration']
result_df['elapsed 14'] = ((current_date - pd.DateOffset(days=14)) - result_df['Start Date']).dt.days / 7 / result_df['Duration']
result_df['elapsed 30'] = ((current_date - pd.DateOffset(days=30)) - result_df['Start Date']).dt.days / 7 / result_df['Duration']
result_df['elapsed 60'] = ((current_date - pd.DateOffset(days=60)) - result_df['Start Date']).dt.days / 7 / result_df['Duration']

# Calculate the Target Referrals to Date
result_df['Target Referrals to Date'] = result_df['elapsed 0'] * result_df['Target Referrals']
result_df['Target FOVs to Date'] = result_df['elapsed 14'] * result_df['Target FOVs']
result_df['Target Consents to Date'] = result_df['elapsed 30'] * result_df['Target Consents']
result_df['Target RANDs to Date'] = result_df['elapsed 60'] * result_df['Target Rands']

# Check if Referrals >= Target Referrals to Date
result_df['Monthly Goal Refs'] = result_df['Monthly Goal Refs']
result_df['Monthly Goal Fovs'] = result_df['FOVs'] >= 0.9 * result_df['Target FOVs to Date']
result_df['Monthly Goal Cons'] = result_df['Consents'] >= 0.9 * result_df['Target Consents to Date']
result_df['Monthly Goal RANDs'] = result_df['Enrolled Randomized AP'] >= 0.9 * result_df['Target RANDs to Date']

#Select the desired columns
result_df = result_df[['Active', 'Sponsor', 'Title', 'Costs', 'Target FOV Rate', 'Actual FOV Rate', 'Monthly Goal Refs', 'Monthly Goal Fovs', 'Monthly Goal Cons', 'Monthly Goal RANDs', 'Study Rands', 'DTP Rands']]

# Output the resulting dataframe to a CSV file
result_df.to_csv('C:/Users/q1032269/OneDrive - IQVIA/Documents/Gitz/Monthly Scorecard/scorecard_results.csv', index=False)

# Calculate the total sum of costs
total_costs = result_df['Costs'].sum()

# Calculate the costs proportion
result_df['Costs Proportion'] = (result_df['Costs'] / total_costs).round(4)

# Calculate the "% to target" Actual FOV Rate / Target FOV rate
result_df['% to Target'] = (result_df['Actual FOV Rate'] / result_df['Target FOV Rate']).round(4)
result_df['% to Target'] = result_df['% to Target'].clip(upper=1.0)

# Calculate the weighted average of the overall FOV "% to target" based on the cost proportion
weighted_avg = (result_df['% to Target'] * result_df['Costs Proportion']).sum()

# Create the FOV Targets dataframe
FOV_targets = result_df[['Title', 'Costs', 'Costs Proportion', 'Target FOV Rate', 'Actual FOV Rate', '% to Target']]
FOV_targets['Weighted Avg'] = weighted_avg

FOV_targets.to_csv('C:/Users/q1032269/OneDrive - IQVIA/Documents/Gitz/Monthly Scorecard/FOV_targets.csv', index=False)