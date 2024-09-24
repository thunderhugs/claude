import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.gridspec import GridSpec
from PIL import Image
import requests
from io import BytesIO
import textwrap
import pandas as pd
from pathlib import Path
import logging

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
        
        # Element 2: Image
        image_url = ad['image_url'] if pd.notna(ad['image_url']) else 'https://www.nomadfoods.com/wp-content/uploads/2018/08/placeholder-1-e1533569576673-1500x1500.png'
        response = requests.get(image_url)
        img = Image.open(BytesIO(response.content))
        img_box = ax.get_position()
        fig.figimage(img, img_box.x0 + img_box.width * 0.07, 
                     fig.bbox.ymax - img_box.y0 - img_box.height * 0.6, 
                     width=img_box.width * 0.86, alpha=1)
        
        # Element 3: Destination URL
        ax.text(0.07, 0.37, 'librexia.com', fontsize=8, color='#606770', va='bottom', ha='left')
        
        # Element 4: Title
        ax.text(0.07, 0.33, ad['title'], fontsize=12, fontweight='bold', va='top', ha='left')
        
        # Element 5: Call to Action
        cta = patches.Rectangle((0.07, 0.2), 0.2, 0.05, fill=True, facecolor='#4267B2')
        ax.add_patch(cta)
        ax.text(0.17, 0.225, 'Learn More', color='white', ha='center', va='center', fontsize=10)
        
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
