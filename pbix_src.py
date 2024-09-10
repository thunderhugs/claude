from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import pandas as pd
import snowflake.connector
import os
from io import StringIO
import configparser
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from datetime import datetime, timedelta

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# Build the path to the config.ini file
config_path = os.path.join(script_dir, 'config.ini')

# Snowflake Connection Credentials
config = configparser.ConfigParser()
config.read(config_path)

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
else:
    print(ctx_auth.get_last_error())

# Snowflake connection
ctx = snowflake.connector.connect(
        user = config.get("snowflake", "user"),
        password = config.get("snowflake", "password"),
        account= config.get("snowflake", "account"),
        warehouse = config.get("snowflake", "warehouse"),
        schema = config.get("snowflake", "schema"),
        role = config.get("snowflake", "role"))

cursor = ctx.cursor()

# Build the path to the SQL file
sql_file_path = os.path.join(script_dir, 'rh.sql')

# Read the SQL file
with open(sql_file_path, 'r') as file:
    query_1 = file.read()

# Execute the query
cursor.execute(query_1)

# Fetch all the results
results = cursor.fetchall()

# Get the column names from the cursor description
column_names = [column[0] for column in cursor.description]

# Convert the results to a DataFrame
df = pd.DataFrame(results, columns=column_names)

# Print the DataFrame
print(df)

csv_file_name = "janssen_librexia_rh_details.csv"
sharepoint_folder_path = "https://quintiles.sharepoint.com/:f:/r/sites/Direct_to_Patient-Marketing_Operations/DTP%20Data%20Catalog/Janssen/VENTURA%20(67953964MDD3001,%2067953964MDD3002)?csf=1&web=1&e=bXmduz"
csv_buffer = StringIO()

def upload_file_to_sharepoint(site_url, username, password, folder_path, file_name, file_object):
    ctx_auth = AuthenticationContext(url=site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)

        file_content = file_object.getvalue().encode("utf-8")
        target_folder = ctx.web.get_folder_by_server_relative_url(folder_path)
        target_file = target_folder.upload_file(file_name, file_content)
        ctx.execute_query()
        print(f"File '{file_name}' has been uploaded to SharePoint folder '{folder_path}'.")
    else:
        print(ctx_auth.get_last_error())

def move_file_to_archive_if_exists(site_url, username, password, folder_path, file_name, archive_folder_path):
    ctx_auth = AuthenticationContext(url=site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(site_url, ctx_auth)

        # Get the file
        file_url = f"{folder_path}/{file_name}"
        file = ctx.web.get_file_by_server_relative_url(file_url)
        ctx.load(file)

        try:
            # Try to execute the query to load the file
            ctx.execute_query()
            print(f"File '{file_name}' exists in SharePoint folder '{folder_path}'.")

            # If the file exists, move it to the archive folder with a timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            archive_file_url = f"{archive_folder_path}/{file_name}_{timestamp}"
            file.moveto(archive_file_url, 1)
            ctx.execute_query()
            print(f"File '{file_name}' has been moved to SharePoint folder '{archive_folder_path}' with timestamp '{timestamp}'.")
        except Exception as e:
            # If the file does not exist, an exception will be thrown when executing the query
            if 'File Not Found' in str(e):
                print(f"File '{file_name}' does not exist in SharePoint folder '{folder_path}'. No need to move it to archive.")
            else:
                raise
    else:
        print(ctx_auth.get_last_error())

# Write the DataFrame to the StringIO object
df.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

output_path = config.get("filepath", "path")
output_path = output_path.strip("'")
output_file = os.path.join(output_path, csv_file_name)

# Write the data to a CSV file
with open(output_file, 'w', newline='') as f:
    f.write(csv_buffer.getvalue())

## TMDH DATA PROCESS

# Build the path to the SQL file
sql_file_path_tmdh = os.path.join(script_dir, 'tmdh.sql')

# Read the SQL file
with open(sql_file_path_tmdh, 'r') as file:
    query_2 = file.read()

# Execute the second query
cursor.execute(query_2)

# Fetch all the results
results_2 = cursor.fetchall()

# Get the column names from the cursor description
column_names_2 = [column[0] for column in cursor.description]

# Convert the results to a DataFrame
df_2 = pd.DataFrame(results_2, columns=column_names_2)

# Print the DataFrame
print(df_2)

# Define the name of the second CSV file
csv_file_name_2 = "janssen_librexia_tmdh.csv"

# Write the second DataFrame to a new StringIO object
csv_buffer_2 = StringIO()
df_2.to_csv(csv_buffer_2, index=False)
csv_buffer_2.seek(0)

# Create the output path for the second file
output_file_2 = os.path.join(output_path, csv_file_name_2)

# Write the data to a CSV file
with open(output_file_2, 'w', newline='', encoding='utf-8') as f:
    f.write(csv_buffer_2.getvalue())

# Google Analytics

# Set up credentials
credentials = service_account.Credentials.from_service_account_file(r'C:\Users\q1032269\OneDrive - IQVIA\Documents\config-keys\Quickstart-10783ca848cb.json')

# Create the Analytics Data client
client = BetaAnalyticsDataClient(credentials=credentials)

# Define the request
# Calculate yesterday's date
yesterday = datetime.now() - timedelta(days=1)
end_date = yesterday.strftime("%Y-%m-%d")

request = RunReportRequest(
    property=f"properties/{'405380625'}",
    date_ranges=[DateRange(start_date="2024-09-01", end_date=end_date)],
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

# Run the report
response = client.run_report(request)

# Prepare data for DataFrame
data_ga = []
for row in response.rows:
    data_row = [value.value for value in row.dimension_values]
    data_row.extend([value.value for value in row.metric_values])
    data_ga.append(data_row)

# Create DataFrame
df3 = pd.DataFrame(data_ga, columns=['date', 'country', 'city', 'source', 'medium', 'sessions', 'users', 'activeUsers'])

# Convert date format
df3['date'] = pd.to_datetime(df3['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

# Apply transformations
df3.loc[df3['source'] == 'hmn', 'medium'] = 'hmn-partner#'
df3.loc[df3['source'] == 'google', 'source'] = 'Digital'
df3.loc[df3['medium'] == 'cpc', 'medium'] = 'Google Ads'
df3.loc[df3['source'].isin(['IQVIAmedia', 'fb']), 'source'] = 'Digital'
df3.loc[df3['source'] == 'survey.alchemer.com', ['source', 'medium']] = '(direct)', '(none)'

# Define the name of the second CSV file
csv_file_name_3= "janssen_librexia_ga.csv"

# Write the second DataFrame to a new StringIO object
csv_buffer_3 = StringIO()
df3.to_csv(csv_buffer_3, index=False)
csv_buffer_3.seek(0)

# Create the output path for the second file
output_file_3 = os.path.join(output_path, csv_file_name_3)

# Write the data to a CSV file
with open(output_file_3, 'w', newline='', encoding='utf-8') as f:
    f.write(csv_buffer_3.getvalue())

#------------------------------Pre Screener Data --------------------------------------------

# Build the path to the SQL file
sql_file_path = os.path.join(script_dir, 'sg.sql')

# Read the SQL file
with open(sql_file_path, 'r') as file:
    query_sg = file.read()

# Execute the query
cursor.execute(query_sg)

# Fetch all the results
results_sg = cursor.fetchall()

# Get the column names from the cursor description
column_names_sg = [column[0] for column in cursor.description]

# Convert the results to a DataFrame
df_sg = pd.DataFrame(results_sg, columns=column_names_sg)

# Print the DataFrame
print(df_sg)

# Define the name of the CSV file
csv_file_name_sg = "janssen_librexia_survey_responses.csv"

# Write the DataFrame to a new StringIO object
csv_buffer_sg = StringIO()
df_sg.to_csv(csv_buffer_sg, index=False)
csv_buffer_sg.seek(0)

# Create the output path for the file
output_file_sg = os.path.join(output_path, csv_file_name_sg)

# Write the data to a CSV file
with open(output_file_sg, 'w', newline='', encoding='utf-8') as f:
    f.write(csv_buffer_sg.getvalue())