import pandas as pd
import folium 

# Define a function to choose a color based on the D&I Potential
def get_color(di_potential):
    if di_potential == 'High Potential/High Enrolling':
        return 'green'
    elif di_potential == 'High Potential/Low Enrolling':
        return 'lightgreen'
    elif di_potential == 'Low Potential/Low Enrolling':
        return 'red'
    elif di_potential == 'Low Potential/High Enrolling':
        return 'orange'    
    else:
        return 'blue'  # Default color

# Read input CSV
input_csv = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\dei_data_sites.csv'
output_csv = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\geocoded_clinical_trial_sites_2.csv'

# Load the input data
df = pd.read_csv(output_csv)

print(df.head())

# Initialize a map centered around the median location
map_center = [df['latitude'].median(), df['longitude'].median()]
m = folium.Map(location=map_center, zoom_start=6)

# Add points to the map
for idx, row in df.iterrows():
    if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
        color = get_color(row['D&I Potential'])
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            rise_on_hover=True,
            tooltip=f"{row['Site Number']}, Referrals: {int(row['Total Referrals'])}",
            icon=folium.Icon(color=color, icon='medkit', prefix='fa')
        ).add_to(m)

# Define the HTML for the legend
legend_html = """
<div style="position: fixed; bottom: 50px; left: 50px; width: 250px; height: 200px; border:2px solid grey; z-index:9999; font-size:14px; background-color:white;">
    &nbsp;<b>D&I Potential</b><br>
    <p>&nbsp;<i class="fa fa-map-marker fa-2x" style="color:green"></i>&nbsp;High Potential/High Enrolling</p>
    <p>&nbsp;<i class="fa fa-map-marker fa-2x" style="color:lightgreen"></i>&nbsp;High Potential/Low Enrolling</p>
    <p>&nbsp;<i class="fa fa-map-marker fa-2x" style="color:orange"></i>&nbsp;Low Potential/High Enrolling</p>
    <p>&nbsp;<i class="fa fa-map-marker fa-2x" style="color:red"></i>&nbsp;Low Potential/Low Enrolling</p>
</div>
"""

# Add the legend to the map
m.get_root().html.add_child(folium.Element(legend_html))

# Save the map to an HTML file
map_html = r'C:\Users\q1032269\OneDrive - IQVIA\Documents\Python\Reporting Automation\Janssen\Librexia\_site_heat_map\clinical_trial_sites_map.html'
m.save(map_html)

print(f"Interactive map saved to {map_html}")