import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import random
import time
import folium 

def generate_user_agent():
    base_name = "site_mapper"
    timestamp = str(random.randint(100000, 999999))
    return f"{base_name}_{timestamp}"

# Define a function to choose a color based on the D&I Potential
def get_color(di_potential):
    if di_potential == 'High':
        return 'green'
    elif di_potential == 'Medium':
        return 'orange'
    elif di_potential == 'Low':
        return 'red'
    else:
        return 'blue'  # Default color

# Function to geocode addresses
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

# Read input CSV
input_csv = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\dei_data_sites.csv'
output_csv = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\geocoded_clinical_trial_sites.csv'

# Load the input data
df = pd.read_csv(input_csv)

# Initialize geolocator
geolocator = Nominatim(user_agent=generate_user_agent(), timeout=30 )
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# Load existing geocoded data if available
try:
    geocoded_df = pd.read_csv(output_csv)
    geocode_cache = {row['address']: (row['latitude'], row['longitude']) for idx, row in geocoded_df.iterrows()}
except FileNotFoundError:
    geocoded_df = pd.DataFrame(columns=['address', 'latitude', 'longitude'])
    geocode_cache = {}

# Geocode addresses
# Geocode addresses
df['full_address'] = df['Address'] + ', ' + df['Site City'] + ', ' + df['Zip Code'].astype(str) + ', ' + df['Country']
df['city_country'] = df['Site City'] + ', ' + df['Country']
df['latitude'] = None
df['longitude'] = None

for idx, row in df.iterrows():
    # Try to geocode using the full address
    lat, lon = geocode_address(row['full_address'], geolocator, geocode_cache)
    if lat is None or lon is None:
        # If full address geocoding fails, try with just city and country
        lat, lon = geocode_address(row['city_country'], geolocator, geocode_cache)
    
    # Update the DataFrame with the geocoded latitude and longitude
    df.at[idx, 'latitude'] = lat
    df.at[idx, 'longitude'] = lon

# Combine new geocoded data with existing data and remove duplicates
geocoded_df = pd.concat([geocoded_df, df[['full_address', 'latitude', 'longitude']]]).drop_duplicates('full_address').reset_index(drop=True)

# Save the geocoded data to the output CSV
geocoded_df.to_csv(output_csv, index=False)

print(f"Geocoding completed. Results saved to {output_csv}")

# Initialize a map centered around the median location
map_center = [geocoded_df['latitude'].median(), df['longitude'].median()]
m = folium.Map(location=map_center, zoom_start=6)

# Add points to the map
for idx, row in df.iterrows():
    if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
        color = get_color(row['D&I Potential'])
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=f"{row['Address']}, {row['Site City']}, {row['Country']}",
            icon=folium.Icon(color=color)
        ).add_to(m)

# Save the map to an HTML file
map_html = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\clinical_trial_sites_map.html'
m.save(map_html)

print(f"Interactive map saved to {map_html}")