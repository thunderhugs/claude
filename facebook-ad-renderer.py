import pandas as pd
from pathlib import Path
import html
import logging

def create_ad_leaderboard_html(merged_df: pd.DataFrame, output_dir: Path, max_ads: int = 50):
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
            <div class="ad-url">librexia.com</div>
            <div class="ad-title">{html.escape(ad['title'])}</div>
            <div class="ad-cta">Learn More</div>
            <div class="ad-stats">
                Sessions: {ad['sessions']:,}<br>
                {f"Referrals: {ad['referrals']:,}" if 'referrals' in ad and ad['referrals'] != 0 else ""}
            </div>
        </div>
        """
    
    html_content += """
    </body>
    </html>
    """
    
    output_file = output_dir / "ad_leaderboard.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logging.info(f"Leaderboard saved to {output_file}")
