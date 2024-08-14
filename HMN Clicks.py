from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, FilterExpression
from datetime import datetime, timedelta
import pandas as pd

# Set up credentials
credentials = service_account.Credentials.from_service_account_file(r'C:\Users\q1032269\OneDrive - IQVIA\Documents\config-keys\Quickstart-10783ca848cb.json')

# Build the service
analytics = build('analytics', 'v3', credentials=credentials)

# Get a list of all Google Analytics accounts
accounts = analytics.management().accounts().list().execute()

property_ids = []
if accounts.get('items'):
    # If accounts are found, get a list of all properties for the first account
    for account in accounts['items']:
        properties = analytics.management().webproperties().list(accountId=account['id']).execute()

        if properties.get('items'):
            # If properties are found, add their IDs to the list
            for property in properties['items']:
                property_ids.append(property['id'])

# Create the Analytics Data client
client = BetaAnalyticsDataClient(credentials=credentials)

# Define the request
# Calculate yesterday's date and the date one year ago
yesterday = datetime.now() - timedelta(days=1)
one_year_ago = datetime.now() - timedelta(days=365)
end_date = yesterday.strftime("%Y-%m-%d")
start_date = one_year_ago.strftime("%Y-%m-%d")

# Prepare data for DataFrame
data_ga = []

for property_id in property_ids:
    try:
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
            ],
            dimension_filter=FilterExpression(string_filter=FilterExpression.StringFilter(value="HMN", field_name="source"))
        )

        # Run the report
        response = client.run_report(request)

        for row in response.rows:
            data_row = [value.value for value in row.dimension_values]
            data_row.extend([value.value for value in row.metric_values])
            data_ga.append(data_row)
    except Exception as e:
        print(f"Error occurred with property ID {property_id}: {e}")

# Create DataFrame
df = pd.DataFrame(data_ga, columns=['date', 'country', 'city', 'source', 'medium', 'sessions', 'users', 'activeUsers'])

# Print DataFrame
print(df)

