from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pandas as pd

# Set up credentials
credentials = service_account.Credentials.from_service_account_file(
    r'C:\Users\q1032269\OneDrive - IQVIA\Documents\config-keys\Quickstart-10783ca848cb.json',
    scopes=['https://www.googleapis.com/auth/analytics.readonly']
)

# Create the Analytics Admin API client
admin_api = build('analyticsadmin', 'v1alpha', credentials=credentials)

# Function to list all GA4 properties
def list_ga4_properties():
    properties = []
    next_page_token = None
    
    while True:
        response = admin_api.properties().list(pageToken=next_page_token).execute()
        properties.extend(response.get('properties', []))
        next_page_token = response.get('nextPageToken')
        
        if not next_page_token:
            break
    
    return [prop['name'] for prop in properties]

# Get all GA4 property IDs
property_ids = list_ga4_properties()
print(f"Found {len(property_ids)} GA4 properties")

# Create the Analytics Data client
client = BetaAnalyticsDataClient(credentials=credentials)

# Calculate yesterday's date and the date 30 days ago
end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

# Prepare data for DataFrame
data_ga = []

for property_id in property_ids:
    try:
        request = RunReportRequest(
            property=property_id,
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
            dimension_filter={
                "filter": {
                    "field_name": "source",
                    "string_filter": {
                        "match_type": "EXACT",
                        "value": "HMN"
                    }
                }
            }
        )

        # Run the report
        response = client.run_report(request)

        for row in response.rows:
            data_row = [property_id] + [value.value for value in row.dimension_values]
            data_row.extend([value.value for value in row.metric_values])
            data_ga.append(data_row)
        
        print(f"Data retrieved for property {property_id}")
    except Exception as e:
        print(f"Error occurred with property ID {property_id}: {str(e)}")

# Create DataFrame
df = pd.DataFrame(data_ga, columns=['property_id', 'date', 'country', 'city', 'source', 'medium', 'sessions', 'users', 'activeUsers'])

# Print DataFrame
print(df)

if df.empty:
    print("No data was returned. Please check your property IDs, date range, and filter settings.")
else:
    print(f"Data retrieved successfully. Shape: {df.shape}")
