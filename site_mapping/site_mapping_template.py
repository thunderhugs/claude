import pandas as pd
import folium
from folium import IFrame
from geopy.geocoders import Nominatim
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import time
import pickle
from branca.element import MacroElement
from jinja2 import Template
import numpy as np

# Load the cache from a file
try:
    with open('geocode_cache.pkl', 'rb') as f:
        geocode_cache = pickle.load(f)
except FileNotFoundError:
    geocode_cache = {}

# Read the CSV file
csv_file = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Study Specific\site_lists\active_sites.csv'
df = pd.read_csv(csv_file)
df.columns = df.columns.str.strip()


# Drop the specified columns
df = df.drop(columns=['Warm Transfer Availability', 'Warm Transfer Last Name', 'Warm Transfer Phone Number', 'Warm Transfer Extension', 'Override Site Address'])

# Iterate over each column in the DataFrame
for col in df.columns:
    # Check if there is a corresponding "Override" column
    override_col = "Override " + col
    if override_col in df.columns:
        # Handle specific edge case
        if col == 'Protocol Number' and 'Project: Override Protocol Number' in df.columns:
            df[col] = np.where(df[col].isna(), df['Project: Override Protocol Number'], df[col])
        else:
            # Combine the two columns
            df[col] = np.where(df[col].isna(), df[override_col], df[col])
        # Drop the "Override" column
        df = df.drop(columns=[override_col])

# Handle the case for 'Protocol Number' and 'Project: Override Protocol Number' if not handled inside the loop
if 'Protocol Number' in df.columns and 'Project: Override Protocol Number' in df.columns:
    df['Protocol Number'] = np.where(df['Protocol Number'].isna(), df['Project: Override Protocol Number'], df['Protocol Number'])
    df = df.drop(columns=['Project: Override Protocol Number'])

# Filter the DataFrame based on Protocol Number
df = df[df['Protocol Number'].isin(['67953964MDD3002', '67953964MDD3001'])]

# Create a geolocator object
geolocator = Nominatim(user_agent="agentSmithers", timeout=20)

# List to store the circle polygons and their centers
circles = []
centers = []

# Create a map object
map_object = folium.Map()

# Iterate over the rows of the dataframe
for index, row in df.iterrows():
    # Get the address from the dataframe
    address = f"{row['Site City']}, {row['Site State/County']}, {row['Site Country']}, {row['Site Postal/Zip Code']}"

    # Check if the address is in the cache
    if address in geocode_cache:
        location = geocode_cache[address]
    else:
        # Get the location coordinates
        location = geolocator.geocode(address)
        time.sleep(1)
        # Store the result in the cache
        geocode_cache[address] = location

    if location is not None:
        lat = location.latitude
        lon = location.longitude
        centers.append((lat, lon))

        # Get the radius for referrals in miles and convert it to meters
        radius = row['Radius for Referrals'] * 1609.34

        # Create a circle polygon
        circle = Point(lon, lat).buffer(radius)
        
        # Check if the circle overlaps with any other circle
        overlap = any(circle.intersects(other_circle) for other_circle in circles[:-1])

        # Add the circle to the list of circles after checking for overlaps
        circles.append(circle)

        # Create a circle on the map with different color based on overlap
        if 'Protocol Number' in row and row['Protocol Number'] == '67953964MDD3001':  # Use '==' for comparison
            color = '#fbc531'
        else:
            color = '#00a8ff' 
        folium.Circle(
            location=[lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            tooltip=f"Protocol:{row['Protocol Number']}, Site No.: {row['Site Number']}, Site Name: {row['Site Name']}"
        ).add_to(map_object)
    
    else: 
        print(f"Could not find coordinates for {address}")

# Calculate the mean latitude and longitude values
mean_lat = sum(lat for lat, lon in centers) / len(centers)
mean_lon = sum(lon for lat, lon in centers) / len(centers)

# Set the location and zoom_start of the map_object
map_object.location = [mean_lat, mean_lon]
map_object.zoom_start = 13

# Calculate the bounding box for all the centers
min_lat = min(lat for lat, lon in centers)
max_lat = max(lat for lat, lon in centers)
min_lon = min(lon for lat, lon in centers)
max_lon = max(lon for lat, lon in centers)

# Fit the map to the bounding box of all the points
map_object.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

from branca.element import Template, MacroElement

class LegendControl(MacroElement):
    def __init__(self):
        super(LegendControl, self).__init__()
        self._name = 'LegendControl'
        self._template = Template("""
        {% macro script(this, kwargs) %}
            var legend = L.control({position: 'bottomleft'});
            legend.onAdd = function (map) {
                var div = L.DomUtil.create('div', 'info legend');
                div.innerHTML += '<b>Legend</b><br>';
                div.innerHTML += '<i style="background: #fbc531"></i> Ventura 1<br>';
                div.innerHTML += '<i style="background: #00a8ff"></i> Ventura 2';
                return div;
            };
            legend.addTo({{this._parent.get_name()}});
        {% endmacro %}
        """)

map_object.get_root().header.add_child(folium.Element("""
    <style>
        .legend {
            padding: 6px 8px;
            font: 14px Arial, Helvetica, sans-serif;
            background: white;
            background: rgba(255, 255, 255, 0.8);
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
            border-radius: 5px;
            line-height: 24px;
            color: #555;
        }
        .legend i {
            width: 18px;
            height: 18px;
            float: left;
            margin-right: 8px;
            opacity: 0.7;
        }
    </style>
"""))

# Add the legend to the map
map_object.add_child(LegendControl())

# Save the map as an HTML file
map_object.save(r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Study Specific\Janssen\site_map.html')

html_table = df.to_html(classes='container', index=False)

# Define CSS styles
css_styles = """
body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 0;
    background-color: #f3f3f3;
}
.container {
    width: 100%;
    background-color: #fff;
    padding: 20px;
    border-radius: 5px;
    box-shadow: 0 0 10px rgba(0, 0, 0, 0.15);
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: calc(5px + 1vmin); /* Responsive font size */
}
table th, table td {
    padding: 15px;
    border: 1px solid #ddd;
}
table tbody tr:nth-child(even) {
    background-color: #f3f3f3;
}
"""

# Define HTML structure with CSS styles embedded
html_dt = f"""
<!DOCTYPE html>
<html>
<head>
<style>
{css_styles}
</style>
</head>
<body>
<div class="container">
    {html_table}
</div>
</body>
</html>
"""

# Define the output path
output_path = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Study Specific\Janssen\data_table.html'

# Write the HTML string a file
with open(output_path, 'w') as f:
    f.write(html_dt)

html = """
<!DOCTYPE html>
<html>
<head>
<style>
body {
    font-family: 'Calibri', sans-serif;
    text-align: center;
}
.frame-container {
    display: block; /* Changed from flex to block */
    margin: 10px auto;
    width: 95%;
}
.iframe {
    width: 90%;
    height: 600px;
    margin-bottom: 20px; /* Added to create some space between the iframes */
}
</style>
</head>
<body>
<h1>Janssen Ventura Site Overlap</h1>
<div class="frame-container">
    <iframe class="iframe" src="site_map.html"></iframe>
    <iframe class="iframe" src="data_table.html"></iframe>
</div>
</body>
</html>
"""

# Define the output path
output_path = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Study Specific\Janssen\ventura_sites_mapped.html'

# Write the HTML string to a file
with open(output_path, 'w') as f:
    f.write(html)

# Save the cache to a file
with open('geocode_cache.pkl', 'wb') as f:
    pickle.dump(geocode_cache, f)