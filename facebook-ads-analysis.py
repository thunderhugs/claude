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

def mock_up_facebook_ad(row: pd.Series, stats: pd.DataFrame) -> plt.Figure:
    """Create a mock-up of a Facebook ad using the provided data and statistics."""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Load and display the image
    response = requests.get(row['image_url'])
    img = Image.open(BytesIO(response.content))
    ax.imshow(img)
    
    # Add title and body text
    ax.text(0.5, 1.05, row['title'], ha='center', va='bottom', fontsize=16, fontweight='bold', transform=ax.transAxes)
    ax.text(0.5, -0.05, row['body'], ha='center', va='top', fontsize=12, wrap=True, transform=ax.transAxes)
    
    # Add statistics
    stats_text = "\n".join([f"{milestone}: {value}" for milestone, value in stats.itertuples(index=False)])
    ax.text(0.95, 0.95, stats_text, ha='right', va='top', fontsize=10, transform=ax.transAxes, bbox=dict(facecolor='white', alpha=0.8))
    
    ax.axis('off')
    return fig

def process_and_save_ads(merged_df: pd.DataFrame, output_dir: Path):
    """Process each ad and save mock-ups."""
    for _, row in merged_df.iterrows():
        # Extract statistics for this ad
        stats = merged_df[merged_df['id'] == row['id']][['Milestone', 'Value']]
        
        fig = mock_up_facebook_ad(row, stats)
        fig.savefig(output_dir / f"ad_mockup_{row['id']}.png", bbox_inches='tight')
        plt.close(fig)

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
    df = pd.DataFrame(ads_data)
    
    # Connect to Snowflake and fetch data
    with connect_to_snowflake(config) as ctx:
        fb = execute_snowflake_query(ctx, script_dir / 'fb.sql')
    
    # Merge Facebook and Snowflake data
    merged_df = pd.merge(df, fb, left_on='id', right_on='CONTENT', how='right')
    merged_df = merged_df.drop(['CONTENT'], axis=1)
    
    # Create mock-ups and save results
    output_dir = script_dir / 'output'
    output_dir.mkdir(exist_ok=True)
    
    process_and_save_ads(merged_df, output_dir)
    
    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_dir / 'output.csv', index=False)
    logging.info(f"Results saved to {output_dir}")

if __name__ == "__main__":
    main()
