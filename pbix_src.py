import os
import shutil
from configparser import ConfigParser
from io import StringIO, BytesIO
from datetime import datetime, timedelta
import pandas as pd
import snowflake.connector
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
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
        role=config.get("snowflake", "role"))

def execute_query(cursor, sql_file_path):
    with open(sql_file_path, 'r') as file:
        query = file.read()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

def archive_existing_csvs(output_path):
    csv_files = [f for f in os.listdir(output_path) if f.endswith('.csv') and f != 'nocion_aspire_project_details.csv']
    if csv_files:
        archive_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_folder = os.path.join(output_path, "Archive", f"log_{archive_date}")
        os.makedirs(archive_folder, exist_ok=True)
        
        for csv_file in csv_files:
            shutil.move(os.path.join(output_path, csv_file), os.path.join(archive_folder, csv_file))
        
        print(f"Archived {len(csv_files)} CSV files to {archive_folder}")
    else:
        print("No existing CSV files to archive.")

def write_to_csv(df, output_path, csv_file_name, ctx=None):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    if output_path.startswith('https://'):
        if ctx is None:
            raise ValueError("ClientContext is required to upload to SharePoint")
        
        file_content = csv_buffer.getvalue().encode('utf-8')
        target_url = f"{output_path}/{csv_file_name}"
        print(f"Uploading to SharePoint at {target_url}")
        
        try:
            File.save_binary(ctx, target_url, file_content)
            print(f"Successfully uploaded {csv_file_name} to SharePoint.")
        except Exception as e:
            print(f"Failed to upload {csv_file_name} to SharePoint: {e}")
    else:
        output_file = os.path.join(output_path, csv_file_name)
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            f.write(csv_buffer.getvalue())
        print(f"File saved locally at {output_file}")

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
    
    data_ga = [
        [*[value.value for value in row.dimension_values], *[value.value for value in row.metric_values]]
        for row in response.rows
    ]
    
    df = pd.DataFrame(data_ga, columns=['date', 'country', 'city', 'source', 'medium', 'sessions', 'users', 'activeUsers'])
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    
    return df

def transform_ga_data(df):
    transformations = {
        ('hmn', 'medium'): 'hmn-partner#',
        ('google', 'source'): 'Digital',
        ('cpc', 'medium'): 'Google Ads',
        ('IQVIAmedia', 'source'): 'Digital',
        ('fb', 'source'): 'Digital',
        ('survey.alchemer.com', 'source'): '(direct)',
        ('survey.alchemer.com', 'medium'): '(none)'
    }
    
    for (condition_value, column), new_value in transformations.items():
        mask = df[column] == condition_value
        df.loc[mask, column] = new_value
    
    return df

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')
    config = read_config(config_path)
    
    ctx = connect_to_sharepoint(config)
    if ctx is None:
        print("Failed to connect to SharePoint. Exiting.")
        return

    snowflake_ctx = connect_to_snowflake(config)
    cursor = snowflake_ctx.cursor()
    
    output_path = config.get("filepath", "path").strip("'")
    output_path_sp = config.get("filepath", "sp_path").strip("'")
    
    archive_existing_csvs(output_path)
    
    sp_username = config.get("windows", "user")
    sp_password = config.get("windows", "password")
    site_url = 'https://quintiles.sharepoint.com/sites/Direct_to_Patient-Marketing_Operations'

    # Process different data types
    data_types = {
        'rh': 'nocion_aspire_rh_details.csv',
        'tmdh': 'nocion_aspire_tmdh.csv',
        'sg': 'nocion_aspire_survey_responses.csv'
    }

    for data_type, file_name in data_types.items():
        sql_file_path = os.path.join(script_dir, f'{data_type}.sql')
        df = execute_query(cursor, sql_file_path)
        write_to_csv(df, output_path, file_name)
        write_to_csv(df, output_path_sp, file_name, ctx)

    # Google Analytics data
    credentials_file = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\config-keys\Quickstart-10783ca848cb.json'
    property_id = '453621966'
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = "2024-10-01"
    ga_df = get_ga_data(credentials_file, property_id, start_date, end_date)
    ga_df = transform_ga_data(ga_df)
    write_to_csv(ga_df, output_path, "nocion_aspire_ga.csv")
    write_to_csv(ga_df, output_path_sp, "nocion_aspire_ga.csv", ctx)

if __name__ == "__main__":
    main()
