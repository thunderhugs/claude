import os
import configparser
from io import StringIO
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import pandas as pd
import snowflake.connector
import win32com.client as win32

def load_config():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def connect_to_sharepoint(config):
    username = config.get("windows", "user")
    password = config.get("windows", "password")
    site_url = "https://quintiles.sharepoint.com/sites/Direct_to_Patient-Marketing_Operations"

    ctx_auth = AuthenticationContext(url=site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print(f"Connected to SharePoint site: {web.properties['Title']}")
        return ctx
    else:
        print(ctx_auth.get_last_error())
        return None

def connect_to_snowflake(config):
    return snowflake.connector.connect(
        user=config.get("snowflake", "user"),
        password=config.get("snowflake", "password"),
        account=config.get("snowflake", "account"),
        warehouse=config.get("snowflake", "warehouse"),
        schema=config.get("snowflake", "schema"),
        role=config.get("snowflake", "role")
    )

def retrieve_sharepoint_data(ctx, list_name):
    list_obj = ctx.web.lists.get_by_title(list_name)
    items = list_obj.get_items().execute_query()
    return pd.DataFrame([item.properties for item in items])

def execute_snowflake_query(conn, sql_file_path):
    with open(sql_file_path, 'r') as file:
        query = file.read()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

def preprocess_data(projects, performance):
    projects['Protocol'] = projects['Protocol'].str.strip()
    merged_data = pd.merge(projects, performance, left_on='Protocol', right_on='PROTOCOL', how='inner')
    return merged_data[merged_data['Active'] == True]

def calculate_metrics(df):
    numeric_columns = ['COSTS', 'REFERRALS', 'CONSENTS', 'RANDOMIZED', 'External_x0020_Budget', 'DTPInternalBudget']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    df['CPR'] = df['COSTS'] / df['REFERRALS']
    df['Rand_Revenue'] = df.apply(lambda row: min(row['RANDOMIZED'] * row['Performance_x003a_RandPrice'], 
                                                  row['Target_x0023_Rands'] * row['Performance_x003a_RandPrice']), axis=1)
    df['Consent_Revenue'] = df.apply(lambda row: min(row['CONSENTS'] * row['Performance_x003a_ConsentPrice'], 
                                                     row['Target_x0023_Consents'] * row['Performance_x003a_ConsentPrice']), axis=1)
    df['Referral_Revenue'] = df.apply(lambda row: min(row['REFERRALS'] * row['Performance_x003a_ReferralPrice'] if row['Performance'] else 0, 
                                                      row['Target_x0023_Referrals'] * row['Performance_x003a_ReferralPrice']), axis=1)

    df['Client_Cost'] = df['COSTS'] / df['Markup_x002f_Margin']
    df['Fixed_Fee_Revenue'] = df.apply(lambda row: min(row['Client_Cost'], row['FixedFeeValue']), axis=1)
    df['Media_Profit'] = df['External_x0020_Budget'] - df['DTPInternalBudget']
    df['Cost_Revenue'] = df.apply(lambda row: 0 if row['Performance'] == 1 else 
                                  (row['Media_Profit'] if row['COSTS'] > row['Media_Profit'] 
                                   else row['COSTS'] / row['Markup_x002f_Margin']), axis=1)

    revenue_columns = ['Rand_Revenue', 'Consent_Revenue', 'Referral_Revenue', 'Fixed_Fee_Revenue', 'Cost_Revenue']
    df['Total_Revenue'] = df[revenue_columns].sum(axis=1)
    df['Net_Profit'] = df['Total_Revenue'] - df['COSTS']
    df['Net_Profit_Margin'] = df['Net_Profit'] / df['Total_Revenue']

    return df

def format_output(df):
    whole_number_columns = ['REFERRALS', 'CONSENTS', 'RANDOMIZED']
    df[whole_number_columns] = df[whole_number_columns].applymap('{:,.0f}'.format)

    currency_columns = ['CPR', 'COSTS', 'Cost_Revenue', 'Fixed_Fee_Revenue', 'Referral_Revenue', 
                        'Performance_x003a_ConsentPrice', 'Performance_x003a_RandPrice', 
                        'Consent_Revenue', 'Rand_Revenue', 'Total_Revenue', 'Net_Profit']
    df[currency_columns] = df[currency_columns].applymap('${:,.2f}'.format)
    df['Net_Profit_Margin'] = df['Net_Profit_Margin'].apply('{:.1%}'.format)

    return df

def create_output_dataframe(df):
    columns = [
        'Sponsor', 'Title', 'PrimaryIndication', 'Performance', 'CPR', 'COSTS',
        'Cost_Revenue', 'Fixed_Fee_Revenue', 'REFERRALS', 'Referral_Revenue', 'CONSENTS', 
        'Performance_x003a_ConsentPrice', 'Consent_Revenue', 'RANDOMIZED', 
        'Performance_x003a_RandPrice', 'Rand_Revenue', 'Total_Revenue', 
        'Net_Profit', 'Net_Profit_Margin'
    ]
    column_rename = {
        'PrimaryIndication': 'Indication',
        'Performance_x003a_ConsentPrice': 'Consent Payment', 
        'Performance_x003a_RandPrice': 'Rand Payment', 
        'Total_Revenue': 'Total DTP Revenue', 
        'COSTS': 'Media Cost',
        'Cost_Revenue': 'Media Revenue',
        'Fixed_Fee_Revenue': 'Fixed Fee Revenue',
        'REFERRALS': 'Referrals',
        'CONSENTS': 'Consents',
        'RANDOMIZED': 'Randomized',
    }
    return df[columns].rename(columns=column_rename)

def send_email(df):
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
    mail.Subject = 'DtP: Executive Summary'
    mail.BodyFormat = 2  # HTML format

    html_table = df.to_html(index=False, border=1)
    styled_html_table = f'''
    <style>
    table {{border-collapse: collapse; font-family: Consolas; font-size: 10px;}}
    th, td {{padding: 15px; text-align: left;}}
    th {{background-color: #f2f2f2;}}
    </style>
    {html_table}'''

    mail.HTMLBody = f'''
    <html>
    <p>Hi Team,</p>
    <p>Please find below the summary of active projects performance:</p>
    <br>
    <body>{styled_html_table}</body>
    </html>'''

    mail.Display()

def main():
    config = load_config()
    sharepoint_context = connect_to_sharepoint(config)
    snowflake_connection = connect_to_snowflake(config)

    projects = retrieve_sharepoint_data(sharepoint_context, "Direct to Patient Project Details")
    performance = execute_snowflake_query(snowflake_connection, os.path.join(os.path.dirname(__file__), 'sql.sql'))

    merged_data = preprocess_data(projects, performance)
    calculated_data = calculate_metrics(merged_data)
    formatted_data = format_output(calculated_data)
    output_df = create_output_dataframe(formatted_data)

    send_email(output_df)

if __name__ == "__main__":
    main()
