import requests
import pandas as pd
import os 
import snowflake.connector
import configparser
from typing import Dict, List
from pathlib import Path
import logging
from PIL import Image
from io import BytesIO
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib import patches
import textwrap
import html

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str) -> configparser.ConfigParser:
    """Load configuration from the specified path."""
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def fetch_facebook_ads_data(api_url: str, params: Dict) -> List[Dict]:
    """Fetch Facebook Ads data from the API."""
    response = requests.get(api_url, params=params)
    if response.status_code != 200:
        logging.error(f"Failed to retrieve data: {response.status_code}")
        logging.error(response.text)
        return []
    
    data = response.json()
    ads_data = []
    for ad in data.get('data', []):
        creative = ad.get('creative', {})
        insights = ad.get('insights', {}).get('data', [{}])[0]
        ads_data.append({
            'id': ad.get('id'),
            'title': creative.get('title'),
            'body': creative.get('body'),
            'image_url': creative.get('image_url'),
            'call_to_action_type': creative.get('call_to_action'),
            'reach': insights.get('reach'),
            'impressions': insights.get('impressions'),
            'clicks': insights.get('clicks'),
        })
    #print(ads_data)
    return ads_data

def connect_to_snowflake(config: configparser.ConfigParser) -> snowflake.connector.SnowflakeConnection:
    """Establish a connection to Snowflake."""
    return snowflake.connector.connect(
        user=config.get("snowflake", "user"),
        password=config.get("snowflake", "password"),
        account=config.get("snowflake", "account"),
        warehouse=config.get("snowflake", "warehouse"),
        schema=config.get("snowflake", "schema"),
        role=config.get("snowflake", "role")
    )

def execute_snowflake_query(ctx: snowflake.connector.SnowflakeConnection, query_path: str) -> pd.DataFrame:
    """Execute a Snowflake query from a file and return the results as a DataFrame."""
    with open(query_path, 'r') as file:
        query = file.read()
    #logging.info(f"Executing query: {query}")
    
    cursor = ctx.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

def process_snowflake_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process Snowflake data: filter for Black or African American and pivot the data to sum Sessions and Referrals."""
    
    #print(df.columns)

    # Pivot the data to sum Sessions and Referrals
    df_pivoted = df.pivot_table(
        values='VALUE', #'BAA_VALUE'],
        index='CONTENT', 
        columns='MILESTONE', 
        aggfunc='sum'
    ).reset_index()
    
    df_pivoted = df_pivoted.rename(columns={
        'Sessions': 'sessions',
        'Referrals': 'referrals',
        #'BAA Sessions': 'baa_sessions',
        #'BAA Referrals': 'baa_referrals'
    })

    logging.info(f"Data pivoted: {(df_pivoted.columns)}")

    # Ensure we have both 'sessions' and 'referrals' columns, fill with 0 if missing
    for col in ['sessions', 'referrals']:
        if col not in df_pivoted.columns:
            df_pivoted[col] = 0
    
   
    return df_pivoted

def create_ad_leaderboard(merged_df: pd.DataFrame, output_dir: Path, max_ads: int = 50):
    """Create an HTML leaderboard of Facebook ads with their statistics, sorted by sessions."""
    # Sort ads by sessions in descending order
    sorted_df = merged_df.sort_values(by='sessions', ascending=False).head(max_ads)
    
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Ad Leaderboard</title>
        <style>
            body { font-family: Arial, sans-serif; background-color: #f0f2f5; margin: 0; padding: 20px; }
            .ad { background-color: white; margin-bottom: 20px; padding: 20px; border: 1px solid #dddfe2; width: 500px; }
            .ad-body { margin-bottom: 10px; font-size: 14px; line-height: 1.4; }
            .ad-image { width: 100%; height: 260px; object-fit: cover; margin-bottom: 10px; }
            .ad-url { color: #606770; font-size: 12px; margin-bottom: 5px; }
            .ad-title { font-weight: bold; font-size: 16px; margin-bottom: 10px; }
            .ad-cta { background-color: #4267B2; color: white; padding: 8px 16px; display: inline-block; margin-bottom: 10px; font-size: 14px; border-radius: 4px; }
            .ad-stats { font-size: 12px; color: #606770; }
        </style>
    </head>
    <body>
    """
    
    for _, ad in sorted_df.iterrows():
        html_content += f"""
        <div class="ad">
            <div class="ad-body">{html.escape(ad['body'])}</div>
            <img class="ad-image" src="{html.escape(ad['image_url'])}" alt="Ad Image" onerror="this.onerror=null;this.src='https://www.nomadfoods.com/wp-content/uploads/2018/08/placeholder-1-e1533569576673-1500x1500.png';">
            <div class="ad-url">https://www.librexiaafstudy.com/</div>
            <div class="ad-title">{html.escape(ad['title'])}</div>
            <div class="ad-cta">Learn More</div>
            <div class="ad-stats">
            {f"B/AA Sessions: {ad['baa_sessions']:,}" if 'baa_sessions' in ad and ad['baa_sessions'] != 0 else ""} <br>
            {f"Pre Screener Sessions: {ad['sessions']:,}" if 'sessions' in ad and ad['sessions'] != 0 else ""} <br>
            {f"Referrals: {ad['referrals']:,}" if 'referrals' in ad and ad['referrals'] != 0 else ""}
            </div>
        </div>
        """
    
    html_content += """
    </body>
    </html>
    """
    
    output_file = output_dir / "janssen_librexia_fb_ads_summary.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logging.info(f"Leaderboard saved to {output_file}")

def main():
    script_dir = Path(__file__).parent
    config = load_config(script_dir / 'config.ini')
    
    # Facebook API parameters
    api_url = "https://graph.facebook.com/v20.0/act_296110949645292/ads"
    params = {
        'fields': 'id,creative{title,body,image_url},insights{reach,impressions,clicks}',
        'limit': 100,
        'access_token': config.get("facebook", "access_token")
    }
    
    # Fetch and process Facebook Ads data
    ads_data = fetch_facebook_ads_data(api_url, params)
    if not ads_data:
        logging.error("No data returned from the Facebook API.")
        return
    df_facebook = pd.DataFrame(ads_data)
    
    
    # Connect to Snowflake and fetch data
    with connect_to_snowflake(config) as ctx:
        df_snowflake = execute_snowflake_query(ctx, script_dir / 'fb.sql')
    
    logging.info(f"Snowflake data loaded: {len(df_snowflake.columns)}")

    # Process Snowflake data
    df_snowflake_processed = process_snowflake_data(df_snowflake)
    
    # Merge Facebook and Snowflake data
    merged_df = pd.merge(df_facebook, df_snowflake_processed, left_on='id', right_on='CONTENT', how='right')
    merged_df = merged_df.drop(['CONTENT'], axis=1)
    merged_df = merged_df.dropna(subset=['id'])
    merged_df['sessions'] = pd.to_numeric(merged_df['sessions'], errors='coerce').fillna(0)
    merged_df['referrals'] = pd.to_numeric(merged_df['referrals'], errors='coerce').fillna(0)

    # Create leaderboard and save results
    output_dir = script_dir / 'output'
    output_dir.mkdir(exist_ok=True)
    
    create_ad_leaderboard(merged_df, output_dir)
    
    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_dir / 'output_merged.csv', index=False)
    logging.info(f"Merged data saved to {output_dir / 'output_merged.csv'}")

if __name__ == "__main__":
    main()
