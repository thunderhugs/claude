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
        ads_data.append({
            'id': ad.get('id'),
            'title': creative.get('title'),
            'body': creative.get('body'),
            'image_url': creative.get('image_url'),
            'call_to_action_type': creative.get('call_to_action_type')
        })
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
    
    cursor = ctx.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

def process_snowflake_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process Snowflake data: filter for Black or African American and pivot the data to sum Sessions and Referrals."""
    
    # Pivot the data to sum Sessions and Referrals
    df_pivoted = df.pivot_table(
        values='VALUE', 
        index='CONTENT', 
        columns='MILESTONE', 
        aggfunc='sum'
    ).reset_index()
    
    df_pivoted = df_pivoted.rename(columns={
        'Sessions': 'sessions',
        'Referrals': 'referrals'
    })

    # Ensure we have both 'sessions' and 'referrals' columns, fill with 0 if missing
    for col in ['sessions', 'referrals']:
        if col not in df_pivoted.columns:
            df_pivoted[col] = 0
    
    return df_pivoted

def create_ad_leaderboard(merged_df: pd.DataFrame, output_dir: Path, max_ads: int = 50):
    """Create a leaderboard of Facebook ads with their statistics, sorted by sessions."""
    # Sort ads by sessions in descending order
    sorted_df = merged_df.sort_values(by='sessions', ascending=False).head(max_ads)
    
    num_ads = len(sorted_df)
    rows = (num_ads + 2) // 3  # Number of rows in the grid, rounded up
    
    # Adjust figure size based on the number of rows, ensure it's within limits
    fig = plt.figure(figsize=(15, 5 * rows))
    gs = GridSpec(rows, 3, figure=fig, hspace=0.6, wspace=0.3)
    
    for idx, (_, ad) in enumerate(sorted_df.iterrows()):
        ax = fig.add_subplot(gs[idx // 3, idx % 3])
        
        # Load and display the image
        image_url = ad['image_url'] if pd.notna(ad['image_url']) else 'https://www.nomadfoods.com/wp-content/uploads/2018/08/placeholder-1-e1533569576673-1500x1500.png'
        response = requests.get(image_url)
        img = Image.open(BytesIO(response.content))
        ax.imshow(img)

        # Add semi-transparent overlay for better text visibility
        overlay = patches.Rectangle((0, 0), 1, 1, transform=ax.transAxes, alpha=0.6, facecolor='white')
        ax.add_patch(overlay)
        
        #Function to wrap text
        def wrap_text(text, width=30):
            return textwrap.fill(text, width)

        # Add body text at the top, left aligned
        wrapped_body = wrap_text(ad['body'], 60)
        ax.text(0, 1.15, wrapped_body, ha='left', va='top', fontsize=10, wrap=True, transform=ax.transAxes, bbox=dict(facecolor='white', alpha=0.8, edgecolor='none', pad=3))
        
        # Add hardcoded URL and title below the image, with a grey background
        url_and_title = f"librexia.com\n{ad['title']}"
        ax.text(0, -0.15, url_and_title, ha='left', va='top', fontsize=10, wrap=True, transform=ax.transAxes, bbox=dict(facecolor='grey', alpha=0.8, edgecolor='none', pad=3))
        
        # Add call to action as a button to the right
        ax.text(0.95, -0.15, ad['call_to_action_type'], ha='right', va='top', fontsize=10, wrap=True, transform=ax.transAxes, bbox=dict(facecolor='blue', alpha=0.8, edgecolor='none', pad=3))
        
        # Add statistics
        stats_text = f"Sessions: {ad['sessions']:,}"
        if ad['referrals'] != 0:
            stats_text += f"\nReferrals: {ad['referrals']:,}"
        ax.text(0.95, 0.95, stats_text, ha='right', va='top', fontsize=10, transform=ax.transAxes, bbox=dict(facecolor='white', alpha=0.8, edgecolor='none', pad=3))
        
        ax.axis('off')
    
    plt.tight_layout(pad=3.0, h_pad=3.0, w_pad=3.0)
    plt.savefig(output_dir / "ad_leaderboard.png", dpi=300, bbox_inches='tight')
    plt.close(fig)
    logging.info(f"Leaderboard saved to {output_dir / 'ad_leaderboard.png'}")

def main():
    script_dir = Path(__file__).parent
    config = load_config(script_dir / 'config.ini')
    
    # Facebook API parameters
    api_url = "https://graph.facebook.com/v18.0/act_296110949645292/ads"
    params = {
        'fields': 'id,creative{title,body,image_url}',
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
