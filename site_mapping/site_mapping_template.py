import pandas as pd
import folium
from folium import IFrame
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from shapely.geometry import Point
import time
import pickle
from branca.element import MacroElement
from jinja2 import Template
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_user_agent():
    base_name = "LibrexiaSiteMapper"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{base_name}_{timestamp}"

def load_geocode_cache(filename='geocode_cache.pkl'):
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return {}

def save_geocode_cache(cache, filename='geocode_cache.pkl'):
    with open(filename, 'wb') as f:
        pickle.dump(cache, f)

def load_data(csv_file):
    df = pd.read_csv(csv_file)
    df.columns = df.columns.str.strip()
    return df

def validate_data(df):
    required_columns = ['Protocol Number', 'Site Number', 'Site Name', 'Site City', 'Country', 'Zip Code', 'Total Referrals', 'D&I Potential']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    # Check for null values in important columns
    null_counts = df[required_columns].isnull().sum()
    if null_counts.sum() > 0:
        logging.warning(f"Null values found in the following columns:\n{null_counts[null_counts > 0]}")

    # Check for non-numeric values in 'Total Referrals'
    non_numeric = df[pd.to_numeric(df['Total Referrals'], errors='coerce').isnull()]
    if not non_numeric.empty:
        logging.warning(f"Non-numeric values found in 'Total Referrals' column for {len(non_numeric)} rows")

    return df

def get_location(address, geolocator, geocode_cache):
    if address in geocode_cache:
        return geocode_cache[address]
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            location = geolocator.geocode(address)
            time.sleep(1)
            if location:
                geocode_cache[address] = location
                logging.info(f"Successfully geocoded: {address}")
                return location
            else:
                logging.warning(f"No results found for address: {address}")
                return None
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            if attempt < max_retries - 1:
                logging.warning(f"Geocoding attempt {attempt + 1} failed for {address}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logging.error(f"Failed to geocode {address} after {max_retries} attempts: {str(e)}")
                return None

def create_circle(row, lat, lon):
    radius = row['Total Referrals'] * 100  # Adjusted for visibility, you may need to fine-tune this
    color = get_circle_color(row)
    return folium.Circle(
        location=[lat, lon],
        radius=radius,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.7,
        popup=f"Protocol: {row['Protocol Number']}<br>Site No.: {row['Site Number']}<br>Site Name: {row['Site Name']}",
    )

def get_circle_color(row):
    if 'D&I Potential' in row:
        if row['D&I Potential'] == 'High Potential/Low Enrolling':
            return '#f1c40f'
        elif row['D&I Potential'] == 'High Potential/High Enrolling':
            return '#27ae60'
        elif row['D&I Potential'] == 'Low Potential/High Enrolling':
            return '#f39c12'
    return '#e74c3c'

def create_map(df, geocode_cache):
    user_agent = generate_user_agent()
    geolocator = Nominatim(user_agent=user_agent, timeout=50)
    map_object = folium.Map(location=[0, 0], zoom_start=2)  # Start with a world view
    
    geocoding_success = 0
    geocoding_failure = 0

    for index, row in df.iterrows():
        address = f"{row['Site City']}, {row['Country']}, {row['Zip Code']}"
        location = get_location(address, geolocator, geocode_cache)

        if location is not None:
            lat, lon = location.latitude, location.longitude
            circle = create_circle(row, lat, lon)
            circle.add_to(map_object)
            geocoding_success += 1
            
            # Add a marker for debugging
            folium.Marker([lat, lon], popup=f"Debug: {address}").add_to(map_object)
        else:
            logging.warning(f"Could not find coordinates for {address}")
            geocoding_failure += 1

    logging.info(f"Geocoding results: {geocoding_success} successes, {geocoding_failure} failures")
    return map_object

def create_data_table(df):
    table_rows = []
    for index, row in df.iterrows():
        table_rows.append(f"""
        <tr id="row-{index}">
            <td>{row['Protocol Number']}</td>
            <td>{row['Site Number']}</td>
            <td>{row['Site Name']}</td>
            <td>{row['Site City']}</td>
            <td>{row['Country']}</td>
            <td>{row['Zip Code']}</td>
            <td>{row['Total Referrals']}</td>
            <td>{row['D&I Potential']}</td>
        </tr>
        """)
    
    table_html = f"""
    <table id="data-table">
        <thead>
            <tr>
                <th>Protocol Number</th>
                <th>Site Number</th>
                <th>Site Name</th>
                <th>Site City</th>
                <th>Country</th>
                <th>Zip Code</th>
                <th>Total Referrals</th>
                <th>D&I Potential</th>
            </tr>
        </thead>
        <tbody>
            {"".join(table_rows)}
        </tbody>
    </table>
    """
    return table_html

def create_single_html(map_object, data_table_html):
    map_html = map_object.get_root().render()
    
    css_styles = """
    body {
        font-family: Arial, sans-serif;
        margin: 0;
        padding: 0;
        background-color: #f3f3f3;
    }
    .container {
        display: flex;
        flex-direction: column;
        align-items: center;
        padding: 20px;
    }
    #map-container {
        width: 90%;
        height: 600px;
        margin-bottom: 20px;
    }
    #data-table-container {
        width: 90%;
        overflow-x: auto;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: calc(5px + 1vmin);
    }
    table th, table td {
        padding: 15px;
        border: 1px solid #ddd;
    }
    table tbody tr:nth-child(even) {
        background-color: #f3f3f3;
    }
    """
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Janssen Librexia Site Overlap</title>
        <style>{css_styles}</style>
    </head>
    <body>
        <div class="container">
            <h1>Janssen Librexia Site Overlap</h1>
            <div id="map-container">
                {map_html}
            </div>
            <div id="data-table-container">
                {data_table_html}
            </div>
        </div>
    </body>
    </html>
    """
    
    return html_content

def main():
    csv_file = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\dei_data_sites.csv'
    output_file = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\librexia_sites_mapped.html'

    try:
        geocode_cache = load_geocode_cache()
        df = load_data(csv_file)
        df = validate_data(df)
        
        map_object = create_map(df, geocode_cache)
        data_table_html = create_data_table(df)
        
        single_html = create_single_html(map_object, data_table_html)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(single_html)
        
        save_geocode_cache(geocode_cache)
        logging.info(f"Successfully generated map and saved to {output_file}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
