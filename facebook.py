import requests
import pandas as pd
import os 
import snowflake.connector
import configparser

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# Build the path to the config.ini file
config_path = os.path.join(script_dir, 'config.ini')

# Snowflake Connection Credentials
config = configparser.ConfigParser()
config.read(config_path)

# Define the API endpoint and access token
api_url = "https://graph.facebook.com/v18.0/act_296110949645292/ads"
params = {
    'fields': 'id,creative{title,body,image_url}',
    'limit': 100,
    'access_token': 'EAAUG3b4IXZCgBOxCXeGqbZAc9ohqf3bF1yDXGQY0Rg0VKfB95AjjpTHZCM9FsYOVAul2Sfg3ZCayxwrxihpo5i8zN3ZAYYLOR3LhnNHH8aZCgZCRZCq5a9XGL9S3PbeQZAHVIcwCL9UrJ77YYyZAsH1jxhHlP4jBCzTvoEnlGzmNIe2XGolUVxZCwsej8ASCcCJgMnWxIxXapFiObyZBFYKDzn3mEJdG'  # Replace 'YOUR_ACCESS_TOKEN' with your actual token
}

# Make the API request
response = requests.get(api_url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    
    # Parse the data
    ads_data = []
    for ad in data.get('data', []):
        ad_id = ad.get('id')
        creative = ad.get('creative', {})
        title = creative.get('title')
        body = creative.get('body')
        image_url = creative.get('image_url')
        ads_data.append({
            'id': ad_id,
            'title': title,
            'body': body,
            'image_url': image_url
        })

    # Create a DataFrame
    df = pd.DataFrame(ads_data)
    
    # Display the DataFrame
    #print(df)
else:
    print(f"Failed to retrieve data: {response.status_code}")
    print(response.text)

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
sql_file_path = os.path.join(script_dir, 'fb.sql')

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
fb = pd.DataFrame(results, columns=column_names)

# Join df and fb on id = content
merged_df = pd.merge(df, fb, left_on='id', right_on='CONTENT', how='right')

# Drop columns 'content' and 'id' from merged_df
merged_df = merged_df.drop(['CONTENT', 'id'], axis=1)

# Print the merged DataFrame
print(merged_df)

# Save the merged DataFrame to a CSV file
output_path = os.path.join(script_dir, 'output.csv')
merged_df.to_csv(output_path, index=False)