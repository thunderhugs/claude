import pandas as pd
import folium
from folium import IFrame
from geopy.geocoders import Nominatim
from shapely.geometry import Point
import time
import pickle
from branca.element import MacroElement
from jinja2 import Template
import json
from datetime import datetime

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

def get_location(address, geolocator, geocode_cache):
    if address in geocode_cache:
        return geocode_cache[address]
    location = geolocator.geocode(address)
    time.sleep(1)
    geocode_cache[address] = location
    return location

def create_circle(row, lat, lon):
    radius = row['Total Referrals'] * 1609.34
    color = get_circle_color(row)
    return folium.Circle(
        location=[lat, lon],
        radius=radius,
        color=color,
        fill=True,
        fill_color=color,
        tooltip=f"Protocol:{row['Protocol Number']}, Site No.: {row['Site Number']}, Site Name: {row['Site Name']}",
        popup=f"Protocol:{row['Protocol Number']}, Site No.: {row['Site Number']}, Site Name: {row['Site Name']}",
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
    map_object = folium.Map()
    centers = []

    for index, row in df.iterrows():
        address = f"{row['Site City']}, , {row['Country']}, {row['Zip Code']}"
        location = get_location(address, geolocator, geocode_cache)

        if location is not None:
            lat, lon = location.latitude, location.longitude
            centers.append((lat, lon))
            circle = create_circle(row, lat, lon)
            circle.add_to(map_object)
            
            # Add custom JavaScript to handle mouseover events
            circle._template = Template(
                u"""
                {% macro script(this, kwargs) %}
                    var {{this.get_name()}} = L.circle(
                        [{{this.location[0]}}, {{this.location[1]}}],
                        {{this.options}}
                    ).addTo({{this._parent.get_name()}});
                    {{this.get_name()}}.on('mouseover', function (e) {
                        highlightTableRow({{index}});
                    });
                    {{this.get_name()}}.on('mouseout', function (e) {
                        unhighlightTableRow({{index}});
                    });
                {% endmacro %}
                """
            )
        else:
            print(f"Could not find coordinates for {address}")

    fit_map_to_points(map_object, centers)
    add_legend(map_object)
    return map_object

def fit_map_to_points(map_object, centers):
    mean_lat = sum(lat for lat, _ in centers) / len(centers)
    mean_lon = sum(lon for _, lon in centers) / len(centers)
    map_object.location = [mean_lat, mean_lon]
    map_object.zoom_start = 13

    min_lat, max_lat = min(lat for lat, _ in centers), max(lat for lat, _ in centers)
    min_lon, max_lon = min(lon for _, lon in centers), max(lon for _, lon in centers)
    map_object.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

def add_legend(map_object):
    legend_html = """
    <div style='position: fixed; bottom: 50px; left: 50px; width: 220px; height: 120px; 
    border:2px solid grey; z-index:9999; font-size:14px;'>
    <div style='position: relative; top: 3px; left: 3px; background-color: white; padding: 5px;'>
    <b>Legend</b><br>
    <i style='background:#27ae60;'>&nbsp;&nbsp;&nbsp;&nbsp;</i> High Potential/High Enrolling<br>
    <i style='background:#f1c40f;'>&nbsp;&nbsp;&nbsp;&nbsp;</i> High Potential/Low Enrolling<br>
    <i style='background:#f39c12;'>&nbsp;&nbsp;&nbsp;&nbsp;</i> Low Potential/High Enrolling<br>
    <i style='background:#e74c3c;'>&nbsp;&nbsp;&nbsp;&nbsp;</i> Low Potential/Low Enrolling
    </div>
    </div>
    """
    map_object.get_root().html.add_child(folium.Element(legend_html))

def create_data_table(df):
    table_rows = []
    for index, row in df.iterrows():
        table_rows.append(f"""
        <tr id="row-{index}" onmouseover="highlightTableRow({index})" onmouseout="unhighlightTableRow({index})">
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
    .highlighted {
        background-color: #ffff99 !important;
    }
    """
    
    js_script = """
    <script>
    function highlightTableRow(index) {
        var row = document.getElementById('row-' + index);
        if (row) {
            row.classList.add('highlighted');
        }
    }

    function unhighlightTableRow(index) {
        var row = document.getElementById('row-' + index);
        if (row) {
            row.classList.remove('highlighted');
        }
    }
    </script>
    """
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Janssen Librexia Site Overlap</title>
        <style>{css_styles}</style>
        {js_script}
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

    geocode_cache = load_geocode_cache()
    df = load_data(csv_file)
    
    map_object = create_map(df, geocode_cache)
    data_table_html = create_data_table(df)
    
    single_html = create_single_html(map_object, data_table_html)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(single_html)
    
    save_geocode_cache(geocode_cache)

if __name__ == "__main__":
    main()
