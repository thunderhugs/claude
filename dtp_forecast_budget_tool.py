import streamlit as st
import snowflake.connector
import pandas as pd
import pycountry
import plotly.graph_objects as go
import numpy as np
import configparser
from dotenv import load_dotenv
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

st.set_page_config(layout="wide", initial_sidebar_state="expanded", page_icon=None, page_title=None)

st.markdown("""
<style>
    .main-content {
        padding-left: 1rem;
        padding-right: 1rem;
    }
    .reportview-container .main footer {visibility: hidden;}
    .reportview-container .main {
        padding-left: 2rem;
        padding-right: 2rem;
    }
    .stMarkdown {
        padding-bottom: 0;
    }
    .columns-container {
        display: flex;
        flex-direction: row;
        gap: 2rem;
    }
    .column {
        flex-basis: 0;
        flex-grow: 1;
        width: 100%;
    }
    .funnel-budget {
        border: 1px solid #ccc;
        border-radius: 5px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        padding: 0;  /* Remove the padding */
    }
                /* Add the CSS for the tooltip */
    .info-tooltip {
        position: relative;
        display: inline-block;
        border-bottom: 1px dotted black;
    }
    .info-tooltip .tooltiptext {
        visibility: hidden;
        width: 200px;
        background-color: #555;
        color: #fff;
        text-align: center;
        border-radius: 6px;
        padding: 5px;
        position: absolute;
        z-index: 1;
        bottom: 125%;
        left: 50%;
        margin-left: -100px;
        opacity: 0;
        transition: opacity 0.3s;
    }
    .info-tooltip:hover .tooltiptext {
        visibility: visible;
        opacity: 1;
    }
</style>
""", unsafe_allow_html=True)

# Wrap the main content in a container with a margin at the top
with st.container():
    st.markdown('<div class="main-content">', unsafe_allow_html=True)
    st.header("Direct-to-Patient Forecasting & Budget Calculator")
    st.markdown("""
        This application helps you forecast and budget for direct-to-patient (DtP) clinical trials by calculating the cost per referral (CPR) based on historical study data and user inputs. Use the filters and input fields in the sidebar to customize your analysis.
    """)

# Snowflake Connection Credentials
config = configparser.ConfigParser()
config.read("config.ini")

# Load environment variables from a .env file
load_dotenv()

# # Connect to Snowflake database
# conn = snowflake.connector.connect(
#     user = config.get("snowflake", "user", fallback=os.getenv("SNOWFLAKE_USER")),
#     password = config.get("snowflake", "password", fallback=os.getenv("SNOWFLAKE_PASSWORD")),
#     account= config.get("snowflake", "account", fallback=os.getenv("SNOWFLAKE_ACCOUNT")),
#     warehouse = config.get("snowflake", "warehouse", fallback=os.getenv("SNOWFLAKE_WAREHOUSE")),
#     schema = config.get("snowflake", "schema", fallback=os.getenv("SNOWFLAKE_SCHEMA")),
#     role = config.get("snowflake", "role", fallback=os.getenv("SNOWFLAKE_ROLE")))

# Connect to Snowflake database
conn = snowflake.connector.connect(
    user = 'srv_us9pparrpr',
    password = 'OIafqe47dsS!ad',
    account= 'iqviaidporg-prdb_reporting',
    warehouse = 'PRDB_PROD',
    schema = 'PROD_US9_PAR_RPR',
    role = 'PROD_PAR_RPR_SEL')


# Define SQL query to execute
sql_query = os.path.join(script_dir, 'query.sql')
@st.cache_data(show_spinner=False)
def load_data():
    st.spinner(text="Loading Historical Study Data...")
    # Execute the SQL query and load the data into a DataFrame
    data = pd.read_sql_query(sql_query, conn)
    return data

# Call the load_data function to get the cached data
data = load_data()

# Get unique therapy areas
list_of_therapy_areas = data['THERAPY_AREA'].unique().tolist()

# Create the therapy_area_to_indications dictionary from the DataFrame
therapy_area_groups = data.groupby(
    ["THERAPY_AREA", "PRIMARY_INDICATION"]).size().reset_index().drop(columns=[0])
therapy_area_to_indications = therapy_area_groups.groupby(
    "THERAPY_AREA")["PRIMARY_INDICATION"].apply(list).to_dict()


# UI components for filtering
with st.sidebar.expander("Filter by Therapy Area/Primary indication"):
    st.subheader("Therapy Area and Indication")
    therapy_area = st.selectbox(
        "Therapy Area",
        options=["All"] + list_of_therapy_areas,
        key="therapy_area_selectbox"
    )

    # Update the list of primary indications based on the selected therapy area
    if therapy_area != "All":
        list_of_primary_indications = therapy_area_to_indications[therapy_area]
    else:
        list_of_primary_indications = [
            indication
            for indications in therapy_area_to_indications.values()
            for indication in indications
        ]

    primary_indication = st.selectbox(
        "Primary Indication",
        options=["All"] + list_of_primary_indications,
        key="primary_indication_selectbox"
    )

# Apply filters to the data
if therapy_area != "All": data = data[data["THERAPY_AREA"] == therapy_area]
if primary_indication != "All": data = data[data["PRIMARY_INDICATION"] == primary_indication]

# Sum the costs and referrals
sum_costs = data["COSTS"].sum()
sum_referrals = data["REFERRALS"].sum()

# Protocol Complexity
protocol_complexity = "Mid"

# UI components for new input fields
with st.sidebar.expander("Recruitment Inputs"):
    #st.subheader("Study Parameters")
    # Create a function to display the tooltip
    def render_tooltip(text, tooltip_text):
        return f"""
        <div class="info-tooltip">
            {text}
            <span class="tooltiptext">{tooltip_text}</span>
        </div>
        """  
    st.markdown(render_tooltip("Study Patient Goal", "Total Number of Patients being sought for this study as a whole"), unsafe_allow_html=True)
    total_patient_goal = st.number_input("", value=10)
    st.markdown(render_tooltip("Target DtP Contribution (%)", "The % of patients DtP intends to contribute to the Study Patient Goal"), unsafe_allow_html=True)
    dtp_contribution = st.number_input("", min_value=1, max_value=100, value=10)
    recruitment_duration = st.number_input("Recruitment Duration", min_value=1, max_value=12, value=6)
    protocol_complexity = st.selectbox("Protocol Complexity", options=["Lowest", "Low", "Mid", "High", "Highest"], index=2)

    
    # Sum the costs and referrals
sum_costs = data["COSTS"].sum()
sum_referrals = data["REFERRALS"].sum()

# Calculate the CPR based on the summed costs and referrals
cpr = sum_costs / sum_referrals

# Get a comprehensive global country list
iso_to_country = {country.alpha_2: country.name for country in pycountry.countries}

# UI components for selecting countries
with st.sidebar.expander("Country Selection"):
    st.subheader("Countries and Contributions")
    selected_country_names = st.multiselect("Select Countries", options=list(iso_to_country.values()))

    # Initialize country contribution dictionary and total_sites variable
    country_sites = {}
    total_sites = 0

    if selected_country_names:
        st.subheader("Configure Country Contributions")
        for country_name in selected_country_names:
            # Get the ISO 2 code for the selected country
            country = [iso for iso, name in iso_to_country.items() if name == country_name][0]
            country_sites[country] = st.number_input(f"{country_name} Sites", min_value=1, value=1, step=1)
            total_sites += country_sites[country]

# Initialize the country_contribution_perc dictionary
country_contribution_perc = {}

# Normalize country contributions to sum up to 100%
if total_sites > 0:
    for country in country_sites:
        country_contribution_perc[country] = (country_sites[country] / total_sites) * 100


# UI Components for Confiuring Full Funnel
with st.sidebar.expander("Funnel Configuration"):
        
    fov_rate = st.number_input("F.O.V. Rate (%)", min_value=0.5, max_value=20.0, value=4.0, step=0.1)
    consent_rate = st.number_input("Consent Rate (%)", min_value=40.0, max_value=99.0, value=85.0, step=0.1)
    screen_fail_rate = st.number_input("Screen Fail Rate (%)", min_value=1.0, max_value=100.0, value=50.0, step=0.1)

# Calculate average CPR for the US
us_cpr = data[data['COUNTRY'] == 'US']['COUNTRY_CPR'].mean()

for country_datum in data['COUNTRY']:
  print(country_datum)

# Calculate the CPR for each selected country based on their % contribution and modifier
country_cpr_list = []
for country_name in selected_country_names:
    # Get the ISO 2 code for the selected country
    country = [iso for iso, name in iso_to_country.items() if name == country_name][0]
    print(data['COUNTRY'])
    print(country)
    country_data = data[data['COUNTRY'] == country]
    print(country_data)
    if not country_data.empty:
        country_avg_cpr = country_data['COUNTRY_CPR'].mean()
        cpr_modifier = (country_avg_cpr - us_cpr) / us_cpr
    else:
        cpr_modifier = 1  # Default modifier for countries with no data

    weighted_cpr = (1 + cpr_modifier) * cpr
    country_cpr_list.append((country, weighted_cpr, country_contribution_perc.get(country, 0)))

# Initialize num_sites
num_sites = 0

# Check if country_sites is defined and is a dictionary
if isinstance(country_sites, dict):
    # Calculate the total number of patients from country contributions
    num_sites = sum(country_sites.values())

# Initialize the enrollment rate and dtp_volume
dtp_volume = 0

# Calculate the No. Sites weighting multiplier and enrollment rate based on the enrollment rate
if num_sites == 0:
    enrollment_rate = 0  # Set enrollment_rate to 0 to avoid division by zero error
else:
    dtp_volume = total_patient_goal * (dtp_contribution / 100)
    enrollment_rate = dtp_volume / (num_sites * recruitment_duration)

# Define the range boundaries and multipliers
min_enrollment_rate, max_enrollment_rate = 0.01, 10
min_multiplier, max_multiplier = 0, 2

# Calculate the No. Sites weighting multiplier based on the enrollment rate
if enrollment_rate <= min_enrollment_rate:
    num_sites_multiplier = min_multiplier
elif enrollment_rate >= max_enrollment_rate:
    num_sites_multiplier = max_multiplier
else:
    num_sites_multiplier = min_multiplier + (enrollment_rate - min_enrollment_rate) * (max_multiplier - min_multiplier) / (max_enrollment_rate - min_enrollment_rate)

# Update the "Calculate No. Sites weighting multiplier" section
#num_sites_multiplier = multipliers[range_index]

# Create a dictionary with multipliers for protocol complexity
protocol_complexity_dict = {
    "Lowest": -0.3,
    "Low": -0.2,
    "Mid": 0,
    "High": 0.25,
    "Highest": 1,
}

# Determine the protocol complexity multiplier based on user selection
protocol_complexity_multiplier = protocol_complexity_dict[protocol_complexity]

# Apply filters to the data
if therapy_area != "All":
    data = data[data["THERAPY_AREA"] == therapy_area]

if primary_indication != "All":
    data = data[data["PRIMARY_INDICATION"] == primary_indication]

# Apply weightings and multipliers to the CPR
cpr = cpr * (1 + protocol_complexity_multiplier) \
    * (1 + num_sites_multiplier)

# Determine the protocol complexity multiplier based on user selection
protocol_complexity_multiplier = protocol_complexity_dict[protocol_complexity]

# Calculate percentage values based on multipliers
protocol_complexity_percentage = protocol_complexity_dict[protocol_complexity] * 100
num_sites_percentage = num_sites_multiplier * 10

# Calculate the initial CPR based on the summed costs and referrals (without any weightings applied)
initial_cpr = sum_costs / sum_referrals

# Apply weightings and multipliers to the initial CPR to get the final CPR
cpr = initial_cpr * (1 + protocol_complexity_multiplier) * \
    (1 + num_sites_multiplier)

# Calculate the CPR for each selected country based on their % contribution and modifier
country_cpr_list = []
for country_name in selected_country_names:
    # Get the ISO 2 code for the selected country
    country = [iso for iso, name in iso_to_country.items() if name == country_name][0]
    country_data = data[data['COUNTRY'] == country]
    if not country_data.empty:
        country_avg_cpr = country_data['COUNTRY_CPR'].mean()
        cpr_modifier = (country_avg_cpr - us_cpr) / us_cpr
    else:
        cpr_modifier = 1  # Default modifier for countries with no data

    weighted_cpr = (1 + cpr_modifier) * cpr
    country_cpr_list.append((country, weighted_cpr, country_contribution_perc.get(country, 0)))

# Calculate the final CPR as a weighted average of each selected country's CPR
final_cpr = sum(weighted_cpr * contribution / 100 for country, weighted_cpr, contribution in country_cpr_list)

# Create two columns
col1, col2 = st.columns([1, 1])

# Column 1 content
with st.container():
    # Add explanation lines
    st.subheader("CPR Calculation")
    st.write(
        f"CPR Calculation: Sampled from {therapy_area} Therapy Area, {primary_indication} Primary Indication = ${initial_cpr:,.2f}")
    if enrollment_rate != 0:
        st.write(
            f"Enrollment Rate: {enrollment_rate:.2f} (Patients: {dtp_volume}/ Sites: {num_sites}/ Months: {recruitment_duration}) {num_sites_multiplier * 100:.1f}% = ${initial_cpr * (1 + num_sites_multiplier):,.2f}"
        )
    st.write(
        f"Protocol Complexity: {protocol_complexity} ({protocol_complexity_percentage:+.0f}%) = ${cpr:,.2f}")

    # Display the calculated CPR in a card format
    with st.container():
        st.markdown("""
            <style>
                .cpr-card {
                    background-color: #f0f2f6;
                    border-radius: 5px;
                    padding: 10px;
                    text-align: center;
                    width: 50%;
                    margin: 0 auto;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }
            </style>
        """, unsafe_allow_html=True)

    # Display the discrete country-level CPRs and patient volumes set by the % modifiers
    st.subheader("Country-Level CPRs and Patient Volumes")

    # Create an empty DataFrame to store the results
    results = pd.DataFrame(columns=["Country", "Markup", "CPR", "Patients", "Contribution"])

    for country, weighted_cpr, contribution in country_cpr_list:
        patient_volume = total_patient_goal * (dtp_contribution / 100) * (contribution / 100)
        country_data = data[data['COUNTRY'] == country]
        if not country_data.empty:
            country_avg_cpr = country_data['COUNTRY_CPR'].mean()
            cpr_modifier = (country_avg_cpr - us_cpr) / us_cpr
            cpr_markup = cpr_modifier * 100
        else:
            cpr_modifier = 1  # Default modifier for countries with no data
            cpr_markup = 100

        country_name = iso_to_country[country]

        # Append the row to the DataFrame
        row = pd.DataFrame({"Country": [country_name], "Markup": [f"{cpr_markup:+.0f}%"], "CPR": [f"${weighted_cpr:.2f}"], "Patients": [f"{patient_volume:.0f}"], "Contribution": [f"{contribution:.0f}%"]})
        results = pd.concat([results, row], ignore_index=True)

    # Check if final_cpr and dtp_volume are not None before creating the total_row DataFrame
    if final_cpr is not None and dtp_volume is not None:
        total_row = pd.DataFrame({
            "Country": ["Summary"], 
            "Markup": "", 
            "CPR": [f"${final_cpr:.2f}"], 
            "Patients": int(dtp_volume) if dtp_volume else "", 
            "Contribution": [""]
        })
        results = pd.concat([results, total_row], ignore_index=True)

    # Display the DataFrame as a table without the index column
    st.write(results.to_html(index=False), unsafe_allow_html=True)

    with st.container():
        st.markdown("""
            <style>
                .cpr-card {
                    background-color: #f0f2f6;
                    border-radius: 5px;
                    padding: 20px;
                    text-align: center;
                    width: 50%;
                    margin: 0 auto;
                    margin-top: 20px;     /* Add a top margin */
                    margin-bottom: 20px;  /* Add a bottom margin */
                }
            </style>
        """, unsafe_allow_html=True)

        #st.markdown(
        #    f'<div class="cpr-card">Cost Per Referral (CPR): <strong>${final_cpr:.2f}</strong></div>',
        #    unsafe_allow_html=True
        #)

dtp_rands = (total_patient_goal * (dtp_contribution / 100))

with st.container():
    st.markdown("### Funnel & Budget")

    total_referrals = int(dtp_rands / (fov_rate / 100) / (consent_rate / 100) / (1 - (screen_fail_rate / 100)))
    total_consents = int(dtp_rands / (1 - (screen_fail_rate / 100)))
    total_fovs = int(dtp_rands / (consent_rate / 100) / (1 - (screen_fail_rate / 100)))
    cost_per_patient = int(final_cpr / (fov_rate / 100) / (consent_rate / 100) / (1 - (screen_fail_rate / 100)))
    total_budget = int(dtp_rands * cost_per_patient)
    total_internal_media_budget = total_budget * 1.45


    ref_log = total_referrals / 10

    data = {
        "Metric": ["Referrals", "First Office Visits", "Consents", "Rands", "Projected Cost per Patient (Internal)", "Total Media Budget (Internal)"],
        "Value": [total_referrals, total_fovs, total_consents, int(dtp_rands), f"${cost_per_patient:,}", f"${total_budget:,}"]
    }
    df = pd.DataFrame(data)

    # Display the DataFrame as a table without the index column
    st.write(df.to_html(index=False), unsafe_allow_html=True)
