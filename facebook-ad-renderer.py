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
    
    # Adjust figure size based on the number of ads
    fig = plt.figure(figsize=(10, 8 * num_ads))
    
    for idx, (_, ad) in enumerate(sorted_df.iterrows()):
        ax = fig.add_subplot(num_ads, 1, idx + 1)
        
        # Set background color
        ax.set_facecolor('#f0f2f5')
        
        # Create white rectangle for ad content
        ad_rect = patches.Rectangle((0.05, 0.05), 0.9, 0.9, fill=True, facecolor='white', edgecolor='#dddfe2')
        ax.add_patch(ad_rect)
        
        # Element 1: Body
        wrapped_body = textwrap.fill(ad['body'], width=50)
        ax.text(0.07, 0.93, wrapped_body, fontsize=10, va='top', ha='left', wrap=True)
        
        ## Element 2: Image
        image_url = ad['image_url'] if pd.notna(ad['image_url']) else 'https://www.nomadfoods.com/wp-content/uploads/2018/08/placeholder-1-e1533569576673-1500x1500.png'
        response = requests.get(image_url)
        img = Image.open(BytesIO(response.content))
        img_box = ax.get_position()

        # Resize the image
        base_width = img_box.width * 0.86
        if img.size[0] > 0:  # Ensure the original image width is not zero
            wpercent = (base_width / float(img.size[0]))
            hsize = int((float(img.size[1]) * float(wpercent)))
            if hsize > 0:  # Ensure the calculated height is not zero
                img = img.resize((round(base_width), round(hsize)))
            else:
                logging.warning(f"Calculated image height is zero for ad {idx}. Using original image size.")
        else:
            logging.warning(f"Original image width is zero for ad {idx}. Using original image size.")

        # Calculate position for the image
        img_x = img_box.x0 + img_box.width * 0.07
        img_y = fig.bbox.ymax - img_box.y0 - img_box.height * 0.6
        
        fig.figimage(img, img_box.x0 + img_box.width * 0.07, 
                    fig.bbox.ymax - img_box.y0 - img_box.height * 0.6, 
                    alpha=1)
        
        # Element 3: Destination URL
        ax.text(0.07, 0.37, 'librexia.com', fontsize=8, color='#606770', va='bottom', ha='left')
        
        # Element 4: Title
        ax.text(0.07, 0.33, ad['title'], fontsize=12, fontweight='bold', va='top', ha='left')
        
        # Element 5: Call to Action
        cta = patches.Rectangle((0.07, 0.2), 0.2, 0.05, fill=True, facecolor='#4267B2')
        ax.add_patch(cta)
        ax.text(0.17, 0.225, ad['call_to_action_type'], color='white', ha='center', va='center', fontsize=10)
        
        # Element 6: Statistics
        stats_text = f"Sessions: {ad['sessions']:,}"
        if 'referrals' in ad and ad['referrals'] != 0:
            stats_text += f"\nReferrals: {ad['referrals']:,}"
        ax.text(0.07, 0.13, stats_text, fontsize=8, va='top', ha='left')
        
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
