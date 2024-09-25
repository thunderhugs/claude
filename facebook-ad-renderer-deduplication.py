
def deduplicate_ads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate ads based on body and title, aggregating metrics and retaining all unique IDs.
    """
    # Group by body and title
    grouped = df.groupby(['body', 'title'])
    
    # Aggregate metrics, keep all unique IDs, and choose one image URL
    deduplicated = grouped.agg({
        'id': lambda x: list(set(x)),  # Keep all unique IDs as a list
        'image_url': 'first',  # Keep the first image URL
        'call_to_action_type': 'first',  # Keep the first CTA type
        'reach': 'sum',
        'impressions': 'sum',
        'clicks': 'sum'
    }).reset_index()
    
    # Explode the ID list to create separate rows for each unique ID
    deduplicated = deduplicated.explode('id')
    
    return deduplicated

def main():
    # ... (keep the existing code)
    
    # Fetch and process Facebook Ads data
    ads_data = fetch_facebook_ads_data(api_url, params)
    if not ads_data:
        logging.error("No data returned from the Facebook API.")
        return
    df_facebook = pd.DataFrame(ads_data)
    
    # Deduplicate ads
    df_facebook_deduplicated = deduplicate_ads(df_facebook)
    
    # Connect to Snowflake and fetch data
    with connect_to_snowflake(config) as ctx:
        df_snowflake = execute_snowflake_query(ctx, script_dir / 'fb.sql')
    
    logging.info(f"Snowflake data loaded: {len(df_snowflake.columns)}")

    # Process Snowflake data
    df_snowflake_processed = process_snowflake_data(df_snowflake)
    
    # Merge Facebook and Snowflake data
    merged_df = pd.merge(df_facebook_deduplicated, df_snowflake_processed, left_on='id', right_on='CONTENT', how='right')
    merged_df = merged_df.drop(['CONTENT'], axis=1)
    merged_df = merged_df.dropna(subset=['id'])  
   
    # Convert numeric columns
    for col in ['sessions', 'referrals', 'baa_sessions', 'baa_referrals', 'reach', 'impressions', 'clicks']:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)

    # Create leaderboard and save results
    output_dir = script_dir / 'output'
    output_dir.mkdir(exist_ok=True)
    
    create_ad_leaderboard(merged_df, output_dir)
    
    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(output_dir / 'output_merged.csv', index=False)
    logging.info(f"Merged data saved to {output_dir / 'output_merged.csv'}")

if __name__ == "__main__":
    main()
