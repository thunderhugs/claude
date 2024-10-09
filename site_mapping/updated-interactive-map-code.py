import csv
import json
import requests
import time
import pickle

def read_csv(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def generate_html(data):
    # (The HTML generation code remains the same as in the original)
    data_json = json.dumps(data)
    
    html_template = f'''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <!-- (Head content remains the same) -->
    </head>
    <body>
        <!-- (Body content remains the same) -->
    </body>
    </html>
    '''
    return html_template

def geocode_with_cache(address, cache_file='geocode_cache.pkl', user_agent="MyGeocoder", timeout=20):
    # Load the cache
    try:
        with open(cache_file, 'rb') as f:
            geocode_cache = pickle.load(f)
    except FileNotFoundError:
        geocode_cache = {}

    # Check if the address is in the cache
    if address in geocode_cache:
        return geocode_cache[address]

    # If not in cache, geocode the address
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",
        "limit": 1
    }
    headers = {
        "User-Agent": user_agent
    }

    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        results = response.json()
        
        if results:
            location = {
                "latitude": float(results[0]["lat"]),
                "longitude": float(results[0]["lon"])
            }
            # Store the result in the cache
            geocode_cache[address] = location
            
            # Save the updated cache
            with open(cache_file, 'wb') as f:
                pickle.dump(geocode_cache, f)
            
            time.sleep(1)  # Respect Nominatim's usage policy
            return location
        else:
            print(f"No results found for address: {address}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error geocoding address {address}: {e}")
        return None

def main():
    csv_file_path = r'C:\Users\hyper\Downloads\dei_data.csv'  # Update this with your CSV file path
    output_html_path = 'interactive_map.html'  # The output HTML file
    cache_file = 'geocode_cache.pkl'  # Cache file for geocoding results
    user_agent = "InteractiveMapGenerator/1.0"  # Custom user agent
    timeout = 30  # Timeout for geocoding requests

    # Read CSV data
    data = read_csv(csv_file_path)

    # Geocode addresses
    for site in data:
        address = f"{site['Address']}, {site['City']}, {site['State / Province']} {site['Zip Code']}"
        location = geocode_with_cache(address, cache_file=cache_file, user_agent=user_agent, timeout=timeout)
        if location:
            site['Latitude'] = location['latitude']
            site['Longitude'] = location['longitude']
        else:
            print(f"Could not geocode address: {address}")

    # Generate HTML
    html_content = generate_html(data)

    # Write HTML to file
    with open(output_html_path, 'w') as htmlfile:
        htmlfile.write(html_content)

    print(f"Interactive map HTML has been generated: {output_html_path}")

if __name__ == "__main__":
    main()
