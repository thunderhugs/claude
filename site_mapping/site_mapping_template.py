import csv
import json

def read_csv(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def generate_html(data):
    data_json = json.dumps(data)
    
    html_template = f'''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Janssen Librexia Site Overlap</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css"/>
        <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
        <style>
            body {{
                font-family: 'Calibri', sans-serif;
                margin: 0;
                padding: 0;
                display: flex;
                flex-direction: column;
                height: 100vh;
            }}
            h1 {{
                text-align: center;
                margin: 20px 0;
            }}
            #map {{
                flex-grow: 1;
                width: 100%;
            }}
            .info-box {{
                padding: 10px;
                background: white;
                border: 1px solid #ccc;
                border-radius: 5px;
            }}
            .legend {{
                padding: 6px 8px;
                font: 14px Arial, Helvetica, sans-serif;
                background: white;
                background: rgba(255, 255, 255, 0.8);
                box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
                border-radius: 5px;
                line-height: 24px;
                color: #555;
            }}
            .legend i {{
                width: 18px;
                height: 18px;
                float: left;
                margin-right: 8px;
                opacity: 0.7;
            }}
        </style>
    </head>
    <body>
        <h1>Janssen Librexia Site Overlap</h1>
        <div id="map"></div>
        <script>
            const data = {data_json};
            
            const map = L.map('map').setView([39.8283, -98.5795], 4);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            }}).addTo(map);

            function getColor(potential) {{
                switch(potential) {{
                    case 'High Potential/High Enrolling':
                        return '#27ae60';
                    case 'High Potential/Low Enrolling':
                        return '#f1c40f';
                    case 'Low Potential/High Enrolling':
                        return '#f39c12';
                    default:
                        return '#e74c3c';
                }}
            }}

            data.forEach(site => {{
                if (site.Latitude && site.Longitude) {{
                    const color = getColor(site['D&I Potential']);
                    const radius = site['Total Referrals'] * 100;  // Adjust multiplier as needed
                    
                    L.circle([site.Latitude, site.Longitude], {{
                        color: color,
                        fillColor: color,
                        fillOpacity: 0.5,
                        radius: radius
                    }}).addTo(map).bindPopup(`
                        <div class="info-box">
                            <h3>${{site['Site Name']}}</h3>
                            <p><strong>Protocol:</strong> ${{site['Protocol Number']}}</p>
                            <p><strong>Site Number:</strong> ${{site['Site Number']}}</p>
                            <p><strong>Address:</strong> ${{site['Address']}}, ${{site['City']}}, ${{site['State / Province']}} ${{site['Zip Code']}}</p>
                            <p><strong>D&I Potential:</strong> ${{site['D&I Potential']}}</p>
                            <p><strong>Total Referrals:</strong> ${{site['Total Referrals']}}</p>
                        </div>
                    `);
                }}
            }});

            // Add legend
            const legend = L.control({{position: 'bottomright'}});
            legend.onAdd = function (map) {{
                const div = L.DomUtil.create('div', 'info legend');
                div.innerHTML += '<b>D&I Potential</b><br>';
                div.innerHTML += '<i style="background: #27ae60"></i> High Potential/High Enrolling<br>';
                div.innerHTML += '<i style="background: #f1c40f"></i> High Potential/Low Enrolling<br>';
                div.innerHTML += '<i style="background: #f39c12"></i> Low Potential/High Enrolling<br>';
                div.innerHTML += '<i style="background: #e74c3c"></i> Low Potential/Low Enrolling<br>';
                return div;
            }};
            legend.addTo(map);
        </script>
    </body>
    </html>
    '''
    return html_template

def main():
    csv_file_path = r'C:\Users\hyper\Downloads\dei_data.csv'  # Update this with your CSV file path
    output_html_path = 'librexia_sites_mapped.html'  # The output HTML file

    # Read CSV data
    data = read_csv(csv_file_path)

    # Generate HTML
    html_content = generate_html(data)

    # Write HTML to file
    with open(output_html_path, 'w') as htmlfile:
        htmlfile.write(html_content)

    print(f"Interactive map HTML has been generated: {output_html_path}")

if __name__ == "__main__":
    main()
