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

def sum_statistics(merged_df: pd.DataFrame) -> pd.DataFrame:
    """Sum the statistics for each ad and return a new DataFrame."""
    # Group by 'id' and 'Milestone', then sum the 'Value'
    summed_stats = merged_df.groupby(['id', 'MILESTONE'])['VALUE'].sum().unstack(fill_value=0)
    
    # Merge the summed stats back with the original ad data
    ad_data = merged_df.drop_duplicates(subset=['id'])[['id', 'title', 'body', 'image_url']]
    return pd.merge(ad_data, summed_stats, on='id')

def create_ad_leaderboard(summed_df: pd.DataFrame, output_dir: Path):
    """Create a leaderboard of Facebook ads with their summed statistics, sorted by Impressions."""
    # Sort ads by Impressions (screenings) in descending order
    #sorted_df = summed_df.sort_values(by='Impressions', ascending=False)
    
    num_ads = len(summed_df)
    rows = (num_ads + 2) // 3  # Number of rows in the grid, rounded up
    
    fig = plt.figure(figsize=(20, 7 * rows))
    gs = GridSpec(rows, 3, figure=fig, hspace=0.4, wspace=0.2)
    
    for idx, (_, ad) in enumerate(summed_df.iterrows()):
        ax = fig.add_subplot(gs[idx // 3, idx % 3])
        
        # Load and display the image
        response = requests.get(ad['image_url'])
        img = Image.open(BytesIO(response.content))
        ax.imshow(img)
        
        # Add title and body text
        ax.text(0.5, 1.05, ad['title'], ha='center', va='bottom', fontsize=10, fontweight='bold', wrap=True, transform=ax.transAxes)
        ax.text(0.5, -0.05, ad['body'], ha='center', va='top', fontsize=8, wrap=True, transform=ax.transAxes)
        
        # Add summed statistics
        stats_text = f"Impressions: {ad['VALUE']:,}\n"
        stats_text += f"Clicks: {ad['Clicks']:,}\n"
        stats_text += f"Spend: ${ad['Spend']:,.2f}"
        ax.text(0.95, 0.95, stats_text, ha='right', va='top', fontsize=8, transform=ax.transAxes, bbox=dict(facecolor='white', alpha=0.8))
        
        # Add rank
        ax.text(0.05, 0.95, f"Rank: {idx + 1}", ha='left', va='top', fontsize=10, fontweight='bold', transform=ax.transAxes, bbox=dict(facecolor='yellow', alpha=0.8))
        
        ax.axis('off')
    
    plt.tight_layout()
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
    df = pd.DataFrame(ads_data)
    
    print(df.columns)

    # Connect to Snowflake and fetch data
    with connect_to_snowflake(config) as ctx:
        fb = execute_snowflake_query(ctx, script_dir / 'fb.sql')
    
    # Merge Facebook and Snowflake data
    merged_df = pd.merge(df, fb, left_on='id', right_on='CONTENT', how='right')
    merged_df = merged_df.drop(['CONTENT'], axis=1)
    
    # Sum statistics for each ad
    summed_df = sum_statistics(merged_df)
    
    # Create leaderboard and save results
    output_dir = script_dir / 'output'
    output_dir.mkdir(exist_ok=True)
    
    create_ad_leaderboard(summed_df, output_dir)
    
    # Save the summed DataFrame to a CSV file
    summed_df.to_csv(output_dir / 'output_summed.csv', index=False)
    logging.info(f"Summed data saved to {output_dir / 'output_summed.csv'}")

if __name__ == "__main__":
    main()
