import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import random
import time
import folium
from typing import Tuple, Dict, Optional
import argparse

def generate_user_agent() -> str:
    base_name = "site_mapper"
    timestamp = str(random.randint(100000, 999999))
    return f"{base_name}_{timestamp}"

def get_color(di_potential: str) -> str:
    color_map = {
        'High': 'green',
        'Medium': 'orange',
        'Low': 'red'
    }
    return color_map.get(di_potential, 'blue')

def geocode_address(address: str, geolocator, geocode_cache: Dict[str, Tuple[float, float]], max_retries: int = 3) -> Tuple[Optional[float], Optional[float]]:
    if address in geocode_cache:
        return geocode_cache[address]
    
    for _ in range(max_retries):
        try:
            location = geolocator.geocode(address)
            if location:
                result = (location.latitude, location.longitude)
                geocode_cache[address] = result
                return result
            return None, None
        except Exception as e:
            print(f"Error geocoding address {address}: {e}")
            time.sleep(2)
    return None, None

def load_and_prepare_data(input_csv: str) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    df['full_address'] = df['Address'] + ', ' + df['Site City'] + ', ' + df['Zip Code'].astype(str) + ', ' + df['Country']
    df['city_country'] = df['Site City'] + ', ' + df['Country']
    return df

def geocode_dataframe(df: pd.DataFrame, geolocator, geocode_cache: Dict[str, Tuple[float, float]]) -> pd.DataFrame:
    for idx, row in df.iterrows():
        if pd.isnull(row['latitude']) or pd.isnull(row['longitude']):
            lat, lon = geocode_address(row['full_address'], geolocator, geocode_cache)
            if lat is None or lon is None:
                lat, lon = geocode_address(row['city_country'], geolocator, geocode_cache)
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lon
    return df

def create_map(df: pd.DataFrame) -> folium.Map:
    map_center = [df['latitude'].median(), df['longitude'].median()]
    m = folium.Map(location=map_center, zoom_start=6)

    for _, row in df.iterrows():
        if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
            color = get_color(row['D&I Potential'])
            popup_text = f"""
            Address: {row['Address']}, {row['Site City']}, {row['Country']}
            Total Referrals: {row['Total Referrals']}
            D&I Potential: {row['D&I Potential']}
            """
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=5 + (row['Total Referrals'] / 10),  # Adjust the divisor to scale the circles appropriately
                popup=popup_text,
                color=color,
                fill=True,
                fill_color=color
            ).add_to(m)

    return m

def main(input_csv: str, output_csv: str, map_html: str):
    # Initialize geolocator
    geolocator = Nominatim(user_agent=generate_user_agent(), timeout=30)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # Load and prepare data
    df = load_and_prepare_data(input_csv)

    # Load existing geocoded data if available
    try:
        geocoded_df = pd.read_csv(output_csv)
        geocode_cache = {row['full_address']: (row['latitude'], row['longitude']) for _, row in geocoded_df.iterrows() if pd.notnull(row['latitude']) and pd.notnull(row['longitude'])}
        
        # Merge existing geocoded data with input data
        df = df.merge(geocoded_df[['full_address', 'latitude', 'longitude']], on='full_address', how='left', suffixes=('', '_geocoded'))
        df['latitude'] = df['latitude_geocoded'].combine_first(df['latitude'])
        df['longitude'] = df['longitude_geocoded'].combine_first(df['longitude'])
        df = df.drop(columns=['latitude_geocoded', 'longitude_geocoded'])
    except FileNotFoundError:
        geocode_cache = {}

    # Geocode addresses
    df = geocode_dataframe(df, geolocator, geocode_cache)

    # Save the geocoded data to the output CSV
    df.to_csv(output_csv, index=False)
    print(f"Geocoding completed. Results saved to {output_csv}")

    # Create and save map
    m = create_map(df)
    m.save(map_html)
    print(f"Interactive map saved to {map_html}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geocode addresses and create an interactive map.")
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to save the output CSV file")
    parser.add_argument("map_html", help="Path to save the output HTML map file")
    args = parser.parse_args()

    main(args.input_csv, args.output_csv, args.map_html)
