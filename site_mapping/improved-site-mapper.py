import pandas as pd
import folium
from folium.plugins import FastMarkerCluster
import argparse
import json

def get_color(di_potential):
    color_map = {
        'High Potential/High Enrolling': 'green',
        'High Potential/Low Enrolling': 'lightgreen',
        'Low Potential/Low Enrolling': 'red',
        'Low Potential/High Enrolling': 'orange'
    }
    return color_map.get(di_potential, 'blue')

def create_map(df):
    map_center = [df['latitude'].median(), df['longitude'].median()]
    m = folium.Map(location=map_center, zoom_start=6)

    def marker_function(row):
        return folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=f"Site: {row['Site Number']}<br>Referrals: {int(row['Total Referrals'])}<br>D&I Potential: {row['D&I Potential']}",
            icon=folium.Icon(color=get_color(row['D&I Potential']), icon='medkit', prefix='fa')
        )

    FastMarkerCluster(data=df[['latitude', 'longitude', 'Site Number', 'Total Referrals', 'D&I Potential']].itertuples(index=False),
                      callback=marker_function).add_to(m)

    return m

def add_legend(m):
    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; width: 250px; border:2px solid grey; z-index:9999; font-size:14px; background-color:white;">
        <p><strong>D&I Potential</strong></p>
        <p><i class="fa fa-map-marker fa-2x" style="color:green"></i> High Potential/High Enrolling</p>
        <p><i class="fa fa-map-marker fa-2x" style="color:lightgreen"></i> High Potential/Low Enrolling</p>
        <p><i class="fa fa-map-marker fa-2x" style="color:orange"></i> Low Potential/High Enrolling</p>
        <p><i class="fa fa-map-marker fa-2x" style="color:red"></i> Low Potential/Low Enrolling</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

def main(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        required_columns = ['latitude', 'longitude', 'Site Number', 'Total Referrals', 'D&I Potential']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Input CSV must contain columns: {', '.join(required_columns)}")

        m = create_map(df)
        add_legend(m)

        # Embed data in HTML
        data_js = f"var site_data = {df.to_json(orient='records')};"
        m.get_root().header.add_child(folium.Element(f'<script>{data_js}</script>'))

        m.save(output_file)
        print(f"Interactive map saved to {output_file}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create an interactive map of clinical trial sites.")
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument("output_file", help="Path to save the output HTML file")
    args = parser.parse_args()

    main(args.input_file, args.output_file)
