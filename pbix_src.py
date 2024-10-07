import os
import shutil
from configparser import ConfigParser
from io import StringIO
from datetime import datetime, timedelta

import pandas as pd
import snowflake.connector
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest

def read_config(config_path):
    config = ConfigParser()
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
        print("Connected to SharePoint site: {0}".format(web.properties['Title']))
    else:
        print(ctx_auth.get_last_error())

def connect_to_snowflake(config):
    ctx = snowflake.connector.connect(
        user=config.get("snowflake", "user"),
        password=config.get("snowflake", "password"),
        account=config.get("snowflake", "account"),
        warehouse=config.get("snowflake", "warehouse"),
        schema=config.get("snowflake", "schema"),
        role=config.get("snowflake", "role"))
    return ctx

def execute_query(cursor, sql_file_path):
    with open(sql_file_path, 'r') as file:
        query = file.read()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    return df

def archive_existing_csvs(output_path):
    csv_files = [f for f in os.listdir(output_path) if f.endswith('.csv') and f != 'nocion_aspire_project_details.csv']
    if csv_files:
        archive_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_folder = os.path.join(output_path, "Archive", f"log_{archive_date}")
        os.makedirs(archive_folder, exist_ok=True)
        
        for csv_file in csv_files:
            source_path = os.path.join(output_path, csv_file)
            dest_path = os.path.join(archive_folder, csv_file)
            shutil.move(source_path, dest_path)
        
        print(f"Archived {len(csv_files)} CSV files to {archive_folder}")
    else:
        print("No existing CSV files to archive.")

def write_to_csv(df, output_path, csv_file_name, ctx=None):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    if output_path.startswith('https://'):
        # Upload to SharePoint
        if ctx is None:
            raise ValueError("ClientContext is required to upload to SharePoint")
        file_content = csv_buffer.getvalue().encode('utf-8')
        target_url = f"{output_path}/{csv_file_name}"
        File.save_binary_direct(ctx, target_url, file_content)
    else:
        # Save to local file system
        output_file = os.path.join(output_path, csv_file_name)
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            f.write(csv_buffer.getvalue())

def get_ga_data(credentials_file, property_id, start_date, end_date):
    credentials = service_account.Credentials.from_service_account_file(credentials_file)
    client = BetaAnalyticsDataClient(credentials=credentials)
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="country"),
            Dimension(name="city"),
            Dimension(name="source"),
            Dimension(name="medium")
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="activeUsers")
        ]
    )
    
    response = client.run_report(request)
    
    data_ga = []
    for row in response.rows:
        data_row = [value.value for value in row.dimension_values]
        data_row.extend([value.value for value in row.metric_values])
        data_ga.append(data_row)
    
    df = pd.DataFrame(data_ga, columns=['date', 'country', 'city', 'source', 'medium', 'sessions', 'users', 'activeUsers'])
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    
    return df

def transform_ga_data(df):
    df.loc[df['source'] == 'hmn', 'medium'] = 'hmn-partner#'
    df.loc[df['source'] == 'google', 'source'] = 'Digital'
    df.loc[df['medium'] == 'cpc', 'medium'] = 'Google Ads'
    df.loc[df['source'].isin(['IQVIAmedia', 'fb']), 'source'] = 'Digital'
    df.loc[df['source'] == 'survey.alchemer.com', ['source', 'medium']] = '(direct)', '(none)'
    return df

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')
    config = read_config(config_path)
    
    connect_to_sharepoint(config)
    snowflake_ctx = connect_to_snowflake(config)
    cursor = snowflake_ctx.cursor()
    
    output_path = config.get("filepath", "path").strip("'")
    output_path_sp = config.get("filepath", "sp_path").strip("'")
    
    # Archive existing CSVs
    archive_existing_csvs(output_path)
    
    ctx = connect_to_sharepoint(config)

    # RH data
    rh_sql_file_path = os.path.join(script_dir, 'rh.sql')
    rh_df = execute_query(cursor, rh_sql_file_path)
    write_to_csv(rh_df, output_path, "nocion_aspire_rh_details.csv")
    write_to_csv(rh_df, output_path_sp, "nocion_aspire_rh_details.csv", ctx)

    # TMDH data 
    tmdh_sql_file_path = os.path.join(script_dir, 'tmdh.sql')
    tmdh_df = execute_query(cursor, tmdh_sql_file_path)
    write_to_csv(tmdh_df, output_path, "nocion_aspire_tmdh.csv")
    write_to_csv(tmdh_df, output_path_sp, "nocion_aspire_tmdh.csv", ctx)
    
    # Google Analytics data
    credentials_file = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\config-keys\Quickstart-10783ca848cb.json'
    property_id = '453621966'
    yesterday = datetime.now() - timedelta(days=1)
    end_date = yesterday.strftime("%Y-%m-%d")
    start_date = "2024-10-01"
    ga_df = get_ga_data(credentials_file, property_id, start_date, end_date)
    ga_df = transform_ga_data(ga_df)
    write_to_csv(ga_df, output_path, "nocion_aspire_ga.csv")
    write_to_csv(ga_df, output_path_sp, "nocion_aspire_ga.csv", ctx)
        
    # Pre Screener data
    sg_sql_file_path = os.path.join(script_dir, 'sg.sql')
    sg_df = execute_query(cursor, sg_sql_file_path)
    write_to_csv(sg_df, output_path, "nocion_aspire_survey_responses.csv")
    write_to_csv(sg_df, output_path_sp, "nocion_aspire_survey_responses.csv", ctx)
    
if __name__ == "__main__":
    main()

