import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import random
import time

def generate_user_agent():
    base_name = "site_mapper"
    timestamp = str(random.randint(100000, 999999))
    return f"{base_name}_{timestamp}"

def geocode_address(address, geolocator, geocode_cache, max_retries=3):
    if address in geocode_cache:
        return geocode_cache[address]
    
    retries = 0
    while retries < max_retries:
        try:
            location = geolocator.geocode(address)
            if location:
                geocode_cache[address] = (location.latitude, location.longitude)
                return location.latitude, location.longitude
            else:
                return None, None
        except Exception as e:
            print(f"Error geocoding address {address}: {e}")
            retries += 1
            time.sleep(2)  # Wait for 2 seconds before retrying
    return None, None

def load_data(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    try:
        geocoded_df = pd.read_csv(output_csv)
        geocode_cache = {row['address']: (row['latitude'], row['longitude']) for idx, row in geocoded_df.iterrows()}
    except FileNotFoundError:
        geocoded_df = pd.DataFrame(columns=['address', 'latitude', 'longitude'])
        geocode_cache = {}
    return df, geocoded_df, geocode_cache

def geocode_dataframe(df, geolocator, geocode_cache):
    df['full_address'] = df['Address'] + ', ' + df['Site City'] + ', ' + df['Zip Code'].astype(str) + ', ' + df['Country']
    df['city_country'] = df['Site City'] + ', ' + df['Country']
    df['latitude'] = None
    df['longitude'] = None

    for idx, row in df.iterrows():
        lat, lon = geocode_address(row['full_address'], geolocator, geocode_cache)
        if lat is None or lon is None:
            lat, lon = geocode_address(row['city_country'], geolocator, geocode_cache)
        df.at[idx, 'latitude'] = lat
        df.at[idx, 'longitude'] = lon
        df.at[idx, 'Total Referrals'] = row['Total Referrals']
    return df

def save_geocoded_data(df, geocoded_df, output_csv):
    geocoded_df = pd.concat([geocoded_df, df[['Site Number', 'D&I Potential', 'Total Referrals', 'full_address', 'latitude', 'longitude']]]).drop_duplicates('full_address').reset_index(drop=True)
    geocoded_df.to_csv(output_csv, index=False)

def main(input_csv, output_csv):
    geolocator = Nominatim(user_agent=generate_user_agent(), timeout=30 )
    df, geocoded_df, geocode_cache = load_data(input_csv, output_csv)
    df = geocode_dataframe(df, geolocator, geocode_cache)
    save_geocoded_data(df, geocoded_df, output_csv)
    print(f"Geocoding completed. Results saved to {output_csv}")

if __name__ == "__main__":
    input_csv = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\dei_data_sites.csv'
    output_csv = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\geocoded_clinical_trial_sites_2.csv'
    main(input_csv, output_csv)