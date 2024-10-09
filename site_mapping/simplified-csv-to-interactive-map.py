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
                <th>Referrals Total</th>
                <th>B/AA Referrals</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>

    <script>
        const data = {data_json};

        const getColor = (potential) => {{
            switch (potential.toLowerCase()) {{
                case 'high': return '#ff0000';
                case 'mid': return '#ffff00';
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

        function updateDoughnutChart(totalReferrals, baaReferrals, siteName = 'All Sites') {{
            if (doughnutChart) {{
                doughnutChart.destroy();
            }}

            const otherReferrals = totalReferrals - baaReferrals;
            const baaPercentage = (baaReferrals / totalReferrals) * 100;
            const otherPercentage = (otherReferrals / totalReferrals) * 100;

            doughnutChart = new Chart(ctx, {{
                type: 'doughnut',
                data: {{
                    labels: ['B/AA', 'Other'],
                    datasets: [{{
                        data: [baaPercentage, otherPercentage],
                        backgroundColor: ['#f28e2c', '#4e79a7'],
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
                            text: `Referral Distribution - ${{siteName}}`
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    let label = context.label || '';
                                    if (label) {{
                                        label += ': ';
                                    }}
                                    const value = context.parsed;
                                    label += value.toFixed(1) + '%';
                                    return label;
                                }}
                            }}
                        }}
                    }}
                }},
            }});
        }}

        const totalReferrals = data.reduce((sum, site) => sum + parseInt(site["Referrals Total"]), 0);
        const totalBaaReferrals = data.reduce((sum, site) => sum + parseInt(site["B/AA Referrals"]), 0);
        updateDoughnutChart(totalReferrals, totalBaaReferrals);

        const markers = [];

        data.forEach(site => {{
            const lat = parseFloat(site.Latitude);
            const lon = parseFloat(site.Longitude);
            
            const circle = L.circle([lat, lon], {{
                color: getColor(site["D&I Potential"]),
                fillColor: getColor(site["D&I Potential"]),
                fillOpacity: 0.5,
                radius: parseFloat(site.Radius) * 1609.34
            }}).addTo(map);

            circle.bindTooltip(`${{site["Site Name"]}} - Referrals: ${{site["Referrals Total"]}}`);
            circle.bindPopup(`
                <h3>${{site["Site Name"]}}</h3>
                <p>Address: ${{site.Address}}, ${{site.City}}, ${{site["State / Province"]}} ${{site["Zip Code"]}}</p>
                <p>Radius: ${{site.Radius}} miles</p>
                <p>D&I Potential: ${{site["D&I Potential"]}}</p>
                <p>Referrals Total: ${{site["Referrals Total"]}}</p>
                <p>B/AA Referrals: ${{site["B/AA Referrals"]}}</p>
            `);

            circle.on('mouseover', () => {{
                highlightTableRow(site["Site Name"]);
                updateDoughnutChart(parseInt(site["Referrals Total"]), parseInt(site["B/AA Referrals"]), site["Site Name"]);
            }});
            circle.on('mouseout', () => {{
                removeHighlight();
                updateDoughnutChart(totalReferrals, totalBaaReferrals);
            }});

            markers.push(circle);
        }});

        const group = L.featureGroup(markers);
        map.fitBounds(group.getBounds());

        const tableBody = document.querySelector('#dataTable tbody');
        data.forEach(site => {{
            const row = tableBody.insertRow();
            row.insertCell(0).textContent = site["Site Name"];
            row.insertCell(1).textContent = `${{site.Address}}, ${{site.City}}, ${{site["State / Province"]}} ${{site["Zip Code"]}}`;
            row.insertCell(2).textContent = site["D&I Potential"];
            row.insertCell(3).textContent = site["Referrals Total"];
            row.insertCell(4).textContent = site["B/AA Referrals"];

            row.addEventListener('mouseover', () => {{
                highlightTableRow(site["Site Name"]);
                updateDoughnutChart(parseInt(site["Referrals Total"]), parseInt(site["B/AA Referrals"]), site["Site Name"]);
            }});
            row.addEventListener('mouseout', () => {{
                removeHighlight();
                updateDoughnutChart(totalReferrals, totalBaaReferrals);
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
    csv_file_path = r'C:\Users\hyper\Downloads\dei_data.csv'  # Update this with your CSV file path
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
