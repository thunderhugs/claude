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
    <title>Interactive Map with Data Table and Doughnut Chart</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/leaflet.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.7.0/chart.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/leaflet.css" />
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
        #map {{ height: 400px; width: 100%; }}
        #doughnutChart {{ height: 400px; width: 100%; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .highlighted {{ background-color: #ffff99; }}
        .container {{ display: flex; flex-wrap: wrap; gap: 20px; }}
        .map-container {{ flex: 2; min-width: 300px; }}
        .chart-container {{ flex: 1; min-width: 300px; }}
    </style>
</head>
<body>
    <h1>Interactive Map with Data Table and Doughnut Chart</h1>
    <div class="container">
        <div class="map-container">
            <div id="map"></div>
        </div>
        <div class="chart-container">
            <canvas id="doughnutChart"></canvas>
        </div>
    </div>
    <table id="dataTable">
        <thead>
            <tr>
                <th>Study Site Name</th>
                <th>Address</th>
                <th>D&I Potential</th>
                <th>Referrals</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>

    <script>
        const data = {data_json};

        const getColor = (potential) => {{
            switch (potential.toLowerCase()) {{
                case 'high': return '#ff0000';
                case 'medium': return '#ffff00';
                case 'low': return '#00ff00';
                default: return '#808080';
            }}
        }};

        const map = L.map('map').setView([39.8283, -98.5795], 4);
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '© OpenStreetMap contributors'
        }}).addTo(map);

        let doughnutChart;
        const ctx = document.getElementById('doughnutChart').getContext('2d');

        function updateDoughnutChart(referrals, siteName = 'All Sites') {{
            if (doughnutChart) {{
                doughnutChart.destroy();
            }}

            doughnutChart = new Chart(ctx, {{
                type: 'doughnut',
                data: {{
                    labels: ['White', 'B/AA'],
                    datasets: [{{
                        data: [referrals * 0.5, referrals * 0.5],
                        backgroundColor: ['#4e79a7', '#f28e2c'],
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            position: 'top',
                        }},
                        title: {{
                            display: true,
                            text: `DEI Percentage - ${{siteName}}`
                        }},
                    }},
                    elements: {{
                        center: {{
                            text: referrals.toString(),
                            color: '#FF6384',
                            fontStyle: 'Arial',
                            sidePadding: 20,
                            minFontSize: 25,
                            lineHeight: 25
                        }}
                    }}
                }},
            }});
        }}

        Chart.register({{
            id: 'doughnutCenterText',
            beforeDraw: function(chart) {{
                if (chart.config.options.elements.center) {{
                    const ctx = chart.ctx;
                    const centerConfig = chart.config.options.elements.center;
                    const fontStyle = centerConfig.fontStyle || 'Arial';
                    ctx.save();
                    ctx.font = '30px ' + fontStyle;
                    ctx.fillStyle = centerConfig.color || '#000';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'middle';
                    const centerX = (chart.chartArea.left + chart.chartArea.right) / 2;
                    const centerY = (chart.chartArea.top + chart.chartArea.bottom) / 2;
                    ctx.fillText(centerConfig.text, centerX, centerY);
                    ctx.restore();
                }}
            }},
        }});

        const totalReferrals = data.reduce((sum, site) => sum + parseInt(site.Referrals), 0);
        updateDoughnutChart(totalReferrals);

        const geocodeAndAddMarker = async (site) => {{
            const address = `${{site.Address}}, ${{site.City}}, ${{site["State/Province"]}} ${{site["Zip Code"]}}`;
            try {{
                const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${{encodeURIComponent(address)}}`);
                const result = await response.json();
                if (result.length > 0) {{
                    const lat = parseFloat(result[0].lat);
                    const lon = parseFloat(result[0].lon);
                    
                    const circle = L.circle([lat, lon], {{
                        color: getColor(site["D&I Potential"]),
                        fillColor: getColor(site["D&I Potential"]),
                        fillOpacity: 0.5,
                        radius: parseFloat(site.Radius) * 1609.34
                    }}).addTo(map);

                    circle.bindTooltip(`${{site["Study Site Name"]}} - Referrals: ${{site.Referrals}}`);
                    circle.bindPopup(`
                        <h3>${{site["Study Site Name"]}}</h3>
                        <p>Address: ${{address}}</p>
                        <p>Radius: ${{site.Radius}} miles</p>
                        <p>D&I Potential: ${{site["D&I Potential"]}}</p>
                        <p>Referrals: ${{site.Referrals}}</p>
                    `);

                    circle.on('mouseover', () => {{
                        highlightTableRow(site["Study Site Name"]);
                        updateDoughnutChart(parseInt(site.Referrals), site["Study Site Name"]);
                    }});
                    circle.on('mouseout', () => {{
                        removeHighlight();
                        updateDoughnutChart(totalReferrals);
                    }});
                }} else {{
                    console.error(`Geocoding failed for address: ${{address}}`);
                }}
            }} catch (error) {{
                console.error('Error:', error);
            }}
        }};

        Promise.all(data.map(geocodeAndAddMarker))
            .then(() => {{
                const group = new L.featureGroup(map._layers);
                map.fitBounds(group.getBounds());
            }});

        const tableBody = document.querySelector('#dataTable tbody');
        data.forEach(site => {{
            const row = tableBody.insertRow();
            row.insertCell(0).textContent = site["Study Site Name"];
            row.insertCell(1).textContent = `${{site.Address}}, ${{site.City}}, ${{site["State/Province"]}} ${{site["Zip Code"]}}`;
            row.insertCell(2).textContent = site["D&I Potential"];
            row.insertCell(3).textContent = site.Referrals;

            row.addEventListener('mouseover', () => {{
                highlightTableRow(site["Study Site Name"]);
                updateDoughnutChart(parseInt(site.Referrals), site["Study Site Name"]);
            }});
            row.addEventListener('mouseout', () => {{
                removeHighlight();
                updateDoughnutChart(totalReferrals);
            }});
        }});

        function highlightTableRow(siteName) {{
            const rows = tableBody.rows;
            for (let i = 0; i < rows.length; i++) {{
                if (rows[i].cells[0].textContent === siteName) {{
                    rows[i].classList.add('highlighted');
                }} else {{
                    rows[i].classList.remove('highlighted');
                }}
            }}
        }}

        function removeHighlight() {{
            const rows = tableBody.rows;
            for (let i = 0; i < rows.length; i++) {{
                rows[i].classList.remove('highlighted');
            }}
        }}
    </script>
</body>
</html>
    '''
    return html_template

def main():
    csv_file_path = 'path/to/your/data.csv'  # Update this with your CSV file path
    output_html_path = 'interactive_map.html'  # The output HTML file

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
