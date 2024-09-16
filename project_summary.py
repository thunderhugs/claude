import os
import configparser
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.lists.list import List
import pandas as pd
import datetime
import snowflake.connector
import win32com.client as win32

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

def execute_query(cursor, sql_file_path):
    with open(sql_file_path, 'r') as file:
        query = file.read()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    return df

def write_to_csv(df, output_path, csv_file_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    output_file = os.path.join(output_path, csv_file_name)
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        f.write(csv_buffer.getvalue())

sharepoint_context = connect_to_sharepoint()
list_name = "Direct to Patient Project Details"
projects = retrieve_list_data(sharepoint_context, list_name)

cursor = connect_to_snowflake()

#execute sql query based on sql.sql file in folder 
performance = execute_query(cursor, os.path.join(script_dir, 'sql.sql'))

# Remove leading and trailing whitespace from 'Protocol' column
projects['Protocol'] = projects['Protocol'].str.strip()

# Merge 'projects' and 'performance' DataFrames on 'Protocol' column
projects_performance = pd.merge(projects, performance, left_on='Protocol', right_on='PROTOCOL', how='inner')

# Filter rows where 'Active' is True
projects_performance = projects_performance[projects_performance['Active'] == True]

# Convert columns to appropriate data types
columns_to_convert = ['COSTS', 'REFERRALS', 'CONSENTS', 'RANDOMIZED', 'External_x0020_Budget', 'DTPInternalBudget']
for column in columns_to_convert:
    projects_performance[column] = pd.to_numeric(projects_performance[column], errors='coerce')

# Calculate 'CPR'
projects_performance['CPR'] = projects_performance['COSTS'] / projects_performance['REFERRALS']

# Calculate revenues
projects_performance['Rand Revenue'] = projects_performance['RANDOMIZED'] * projects_performance['Performance_x003a_RandPrice']

# Calculate 'Rand Revenue' with a cap
projects_performance['Rand Revenue'] = projects_performance.apply(
    lambda row: min(row['Rand Revenue'], 
                    row['Target_x0023_Rands'] * row['Performance_x003a_RandPrice']), axis=1)

projects_performance['Consent Revenue'] = projects_performance['CONSENTS'] * projects_performance['Performance_x003a_ConsentPrice']

# Calculate 'Consent Revenue' with a cap
projects_performance['Consent Revenue'] = projects_performance.apply(
    lambda row: min(row['Consent Revenue'], 
                    row['Target_x0023_Consents'] * row['Performance_x003a_ConsentPrice']), axis=1)

projects_performance['Referral Revenue'] = projects_performance.apply(lambda x: x['REFERRALS'] * x['Performance_x003a_ReferralPrice'] if x['Performance'] else 0, axis=1)

# Calculate 'Ref Revenue' with a cap
projects_performance['Referral Revenue'] = projects_performance.apply(
    lambda row: min(row['Referral Revenue'], 
                    row['Target_x0023_Referrals'] * row['Performance_x003a_ReferralPrice']), axis=1)

projects_performance['client_cost'] = projects_performance['COSTS'] / projects_performance['Markup_x002f_Margin']

# Calculate 'FixedFeeRev'
projects_performance['FixedFeeRev'] = projects_performance.apply(
    lambda row: row['client_cost'] if row['client_cost'] < row['FixedFeeValue'] else row['FixedFeeValue'], axis=1)

# Replace NaN values with 0
projects_performance = projects_performance.fillna(0)

print(projects_performance.columns)

projects_performance['External_x0020_Budget'] = projects_performance['External_x0020_Budget'].fillna(0.0)
projects_performance['External_x0020_Budget'] = projects_performance['External_x0020_Budget'].astype(float).round(2)
projects_performance['MediaProfit'] = projects_performance['External_x0020_Budget'] - projects_performance['DTPInternalBudget']

# Calculate 'CostRevenue'
projects_performance['CostRevenue'] = projects_performance.apply(
    lambda row: 0 if row['Performance'] == 1 else 
    (row['MediaProfit']) if row['COSTS'] > row['MediaProfit'] 
    else row['COSTS'] / row['Markup_x002f_Margin'], axis=1)

# Calculate 'TotalRevenue', 'NetProfit', and 'NetProfitMargin'
projects_performance['TotalRevenue'] = projects_performance[['Rand Revenue', 'Consent Revenue', 'Referral Revenue', 'FixedFeeRev', 'CostRevenue']].sum(axis=1)
projects_performance['NetProfit'] = projects_performance['TotalRevenue'] - projects_performance['COSTS']
projects_performance['NetProfitMargin'] = projects_performance['NetProfit'] / projects_performance['TotalRevenue']

#Format whole numbers
metrics = ['REFERRALS', 'CONSENTS', 'RANDOMIZED']
projects_performance[metrics] = projects_performance[metrics].map('{:,.0f}'.format)

# Format columns as currency
currency_columns = ['CPR', 'COSTS', 'CostRevenue', 'FixedFeeRev', 'Referral Revenue', 'Performance_x003a_ConsentPrice', 'Performance_x003a_RandPrice', 'Consent Revenue', 'Rand Revenue', 'TotalRevenue', 'NetProfit']
projects_performance[currency_columns] = projects_performance[currency_columns].map('${:,.2f}'.format)
projects_performance['NetProfitMargin'] = projects_performance['NetProfitMargin'].apply('{:.1%}'.format)

# Select and rename columns
projects_performance = projects_performance[
    [
        'Sponsor', 'Title', 'PrimaryIndication', 'Performance', 'CPR', 'COSTS',
        'CostRevenue','FixedFeeRev','REFERRALS', 'Referral Revenue', 'CONSENTS', 
        'Performance_x003a_ConsentPrice', 'Consent Revenue', 'RANDOMIZED', 
        'Performance_x003a_RandPrice', 'Rand Revenue', 'TotalRevenue', 
        'NetProfit', 'NetProfitMargin'
    ]
].rename(
    columns={
        'Primary Indiction': 'Indication',
        'Performance_x003a_ConsentPrice': 'Consent Payment', 
        'Performance_x003a_RandPrice': 'Rand Payment', 
        'TotalRevenue': 'Total DTP Revenue', 
        'COSTS': 'Media Cost',
        'CostRevenue': 'Media Revenue',
        'FixedFeeRev': 'Fixed Fee Revenue',
        'REFERRALS': 'Referrals',
        'CONSENTS': 'Consents',
        'RANDOMIZED': 'Randomized',
    }
)

# Create a new Outlook email
outlook = win32.Dispatch('Outlook.Application')
mail = outlook.CreateItem(0)

# Set the email properties
mail.Subject = 'DtP: Executive Summary'
mail.BodyFormat = 2  # HTML format

# Convert the DataFrame to an HTML table
html_table = projects_performance.to_html(index=False, border=1)

# Add CSS styling to the HTML table
html_table = f'''
<style>
table {{border-collapse: collapse; font-family: Consolas; font-size: 10px;}}
th, td {{padding: 15px; text-align: left;}}
th {{background-color: #f2f2f2;}}
</style>
{html_table}'''

# Embed the HTML table in the email body
mail.HTMLBody = f'''
<html>
<p>Hi Team,</p>
<p>Please find below the summary of active projects performance:</p>
<br>
<body>{html_table}</body></html>'
'''

# Display the email
mail.Display()


