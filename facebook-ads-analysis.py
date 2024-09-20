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
            'image_url': creative.get('image_url')
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
    # Filter for Black or African American
    df_filtered = df[df['RACE'] == 'Black or African American']
    
    # Pivot the data to sum Sessions and Referrals
    df_pivoted = df_filtered.pivot_table(
        values='VALUE', 
        index='CONTENT', 
        columns='MILESTONE', 
        aggfunc='sum'
    ).reset_index()
    
    # Ensure we have both 'sessions' and 'referrals' columns, fill with 0 if missing
    for col in ['sessions', 'referrals']:
        if col not in df_pivoted.columns:
            df_pivoted[col] = 0
    
    return df_pivoted

def create_ad_leaderboard(merged_df: pd.DataFrame, output_dir: Path):
    """Create a leaderboard of Facebook ads with their statistics, sorted by sessions."""
    # Sort ads by sessions in descending order
    sorted_df = merged_df.sort_values(by='sessions', ascending=False)
    
    num_ads = len(sorted_df)
    rows = (num_ads + 2) // 3  # Number of rows in the grid, rounded up
    
    fig = plt.figure(figsize=(30, 12 * rows))  # Increased figure size
    gs = GridSpec(rows, 3, figure=fig, hspace=0.6, wspace=0.3)  # Increased spacing
    
    for idx, (_, ad) in enumerate(sorted_df.iterrows()):
        ax = fig.add_subplot(gs[idx // 3, idx % 3])
        
        # Load and display the image
        response = requests.get(ad['image_url'])
        img = Image.open(BytesIO(response.content))
        ax.imshow(img)
        
        # Add semi-transparent overlay for better text visibility
        overlay = patches.Rectangle((0, 0), 1, 1, transform=ax.transAxes, alpha=0.6, facecolor='white')
        ax.add_patch(overlay)
        
        # Add title
        ax.text(0.5, 1.05, ad['title'], ha='center', va='bottom', fontsize=12, fontweight='bold', wrap=True, transform=ax.transAxes)
        
        # Add body text
        ax.text(0.5, -0.15, ad['body'], ha='center', va='top', fontsize=10, wrap=True, transform=ax.transAxes)
        
        # Add statistics
        stats_text = f"Sessions: {ad['sessions']:,}\nReferrals: {ad['referrals']:,}"
        ax.text(0.95, 0.95, stats_text, ha='right', va='top', fontsize=10, transform=ax.transAxes, bbox=dict(facecolor='white', alpha=0.8, edgecolor='none', pad=3))
        
        # Add rank
        ax.text(0.05, 0.95, f"Rank: {idx + 1}", ha='left', va='top', fontsize=12, fontweight='bold', transform=ax.transAxes, bbox=dict(facecolor='yellow', alpha=0.8, edgecolor='none', pad=3))
        
        ax.axis('off')
    
    plt.tight_layout(pad=3.0, h_pad=3.0, w_pad=3.0)  # Increased padding
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
    
    # Create leaderboard and save results
    output_dir = script_dir / 'output'
    output_dir.mkdir(exist_ok=True)
    
    create_ad_leaderboard(merged_df, output_dir)
    
    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_dir / 'output_merged.csv', index=False)
    logging.info(f"Merged data saved to {output_dir / 'output_merged.csv'}")

if __name__ == "__main__":
    main()
