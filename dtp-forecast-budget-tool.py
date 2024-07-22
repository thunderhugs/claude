import streamlit as st
import snowflake.connector
import pandas as pd
import pycountry
import plotly.graph_objects as go
import numpy as np
import configparser
from dotenv import load_dotenv
import os

# Configuration and setup
st.set_page_config(layout="wide", initial_sidebar_state="expanded", page_icon=None, page_title=None)

# Custom CSS (unchanged)
st.markdown("""
<style>
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
</style>
""", unsafe_allow_html=True)

# Load environment variables and connect to Snowflake
load_dotenv()
config = configparser.ConfigParser()
config.read("config.ini")

# Snowflake connection (using environment variables)
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

# Load data
@st.cache_data(show_spinner=False)
def load_data():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    sql_query = os.path.join(script_dir, 'query.sql')
    with st.spinner(text="Loading Historical Study Data..."):
        return pd.read_sql_query(sql_query, conn)

data = load_data()

# UI components
st.header("Direct-to-Patient Forecasting & Budget Calculator")
st.markdown("""
    This application helps you forecast and budget for direct-to-patient (DtP) clinical trials by calculating 
    the cost per referral (CPR) based on historical study data and user inputs. Use the filters and input 
    fields in the sidebar to customize your analysis.
""")

# Sidebar filters and inputs
with st.sidebar:
    with st.expander("Filter by Therapy Area/Primary indication"):
        therapy_area = st.selectbox(
            "Therapy Area",
            options=["All"] + list(data['THERAPY_AREA'].unique()),
            key="therapy_area_selectbox"
        )
        
        primary_indications = data['PRIMARY_INDICATION'].unique() if therapy_area == "All" else \
                              data[data['THERAPY_AREA'] == therapy_area]['PRIMARY_INDICATION'].unique()
        
        primary_indication = st.selectbox(
            "Primary Indication",
            options=["All"] + list(primary_indications),
            key="primary_indication_selectbox"
        )

    with st.expander("Recruitment Inputs"):
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

    with st.expander("Country Selection"):
        iso_to_country = {country.alpha_2: country.name for country in pycountry.countries}
        selected_country_names = st.multiselect("Select Countries", options=list(iso_to_country.values()))
        
        country_sites = {}
        if selected_country_names:
            for country_name in selected_country_names:
                country = next(iso for iso, name in iso_to_country.items() if name == country_name)
                country_sites[country] = st.number_input(f"{country_name} Sites", min_value=1, value=1, step=1)

    with st.expander("Funnel Configuration"):
        fov_rate = st.number_input("F.O.V. Rate (%)", min_value=0.5, max_value=20.0, value=4.0, step=0.1)
        consent_rate = st.number_input("Consent Rate (%)", min_value=40.0, max_value=99.0, value=85.0, step=0.1)
        screen_fail_rate = st.number_input("Screen Fail Rate (%)", min_value=1.0, max_value=100.0, value=50.0, step=0.1)

# Data processing
filtered_data = data
if therapy_area != "All":
    filtered_data = filtered_data[filtered_data["THERAPY_AREA"] == therapy_area]
if primary_indication != "All":
    filtered_data = filtered_data[filtered_data["PRIMARY_INDICATION"] == primary_indication]

sum_costs = filtered_data["COSTS"].sum()
sum_referrals = filtered_data["REFERRALS"].sum()
initial_cpr = sum_costs / sum_referrals if sum_referrals else 0

# CPR calculations
protocol_complexity_dict = {
    "Lowest": -0.3, "Low": -0.2, "Mid": 0, "High": 0.25, "Highest": 1
}
protocol_complexity_multiplier = protocol_complexity_dict[protocol_complexity]

num_sites = sum(country_sites.values())
dtp_volume = total_patient_goal * (dtp_contribution / 100)
enrollment_rate = dtp_volume / (num_sites * recruitment_duration) if num_sites and recruitment_duration else 0

# Calculate No. Sites weighting multiplier
min_enrollment_rate, max_enrollment_rate = 0.01, 10
min_multiplier, max_multiplier = 0, 2
num_sites_multiplier = min_multiplier + (enrollment_rate - min_enrollment_rate) * (max_multiplier - min_multiplier) / (max_enrollment_rate - min_enrollment_rate)
num_sites_multiplier = max(min(num_sites_multiplier, max_multiplier), min_multiplier)

# Apply weightings and multipliers to get the final CPR
cpr = initial_cpr * (1 + protocol_complexity_multiplier) * (1 + num_sites_multiplier)

# Country-specific CPR calculations
us_cpr = filtered_data[filtered_data['COUNTRY'] == 'US']['COUNTRY_CPR'].mean()
country_cpr_list = []
for country_name in selected_country_names:
    country = next(iso for iso, name in iso_to_country.items() if name == country_name)
    country_data = filtered_data[filtered_data['COUNTRY'] == country]
    if not country_data.empty:
        country_avg_cpr = country_data['COUNTRY_CPR'].mean()
        cpr_modifier = (country_avg_cpr - us_cpr) / us_cpr if us_cpr else 0
    else:
        cpr_modifier = 0
    weighted_cpr = cpr * (1 + cpr_modifier)
    contribution = (country_sites[country] / num_sites * 100) if num_sites else 0
    country_cpr_list.append((country, weighted_cpr, contribution))

final_cpr = sum(weighted_cpr * contribution / 100 for _, weighted_cpr, contribution in country_cpr_list) if country_cpr_list else cpr

# Display results
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("CPR Calculation")
    st.write(f"Base CPR: ${initial_cpr:,.2f}")
    st.write(f"Enrollment Rate: {enrollment_rate:.2f} patients/site/month")
    st.write(f"No. Sites Multiplier: {num_sites_multiplier:.2%}")
    st.write(f"Protocol Complexity: {protocol_complexity} ({protocol_complexity_multiplier:+.0%})")
    st.write(f"Final CPR: ${final_cpr:.2f}")

    # New table for Sampled CPR Channels
    st.subheader("Sampled CPR Channels")
    channel_data = filtered_data.groupby("CHANNEL").agg({
        "COSTS": "sum",
        "REFERRALS": "sum"
    }).reset_index()
    
    channel_data["CPR"] = channel_data["COSTS"] / channel_data["REFERRALS"]
    channel_data["CPR"] = channel_data["CPR"].apply(lambda x: f"${x:.2f}")
    channel_data = channel_data[["CHANNEL", "CPR"]]
    
    st.write(channel_data.to_html(index=False), unsafe_allow_html=True)

    st.subheader("Country-Level CPRs and Patient Volumes")
    results = pd.DataFrame([
        {
            "Country": iso_to_country[country],
            "Markup": f"{((weighted_cpr / cpr - 1) * 100):+.0f}%",
            "CPR": f"${weighted_cpr:.2f}",
            "Patients": f"{dtp_volume * contribution / 100:.0f}",
            "Contribution": f"{contribution:.0f}%"
        }
        for country, weighted_cpr, contribution in country_cpr_list
    ])
    
    if not results.empty:
        results = results.append({
            "Country": "Summary",
            "Markup": "",
            "CPR": f"${final_cpr:.2f}",
            "Patients": f"{dtp_volume:.0f}",
            "Contribution": "100%"
        }, ignore_index=True)
    
    st.write(results.to_html(index=False), unsafe_allow_html=True)

with col2:
    st.subheader("Funnel & Budget")
    total_referrals = int(dtp_volume / (fov_rate / 100) / (consent_rate / 100) / (1 - (screen_fail_rate / 100)))
    total_fovs = int(dtp_volume / (consent_rate / 100) / (1 - (screen_fail_rate / 100)))
    total_consents = int(dtp_volume / (1 - (screen_fail_rate / 100)))
    cost_per_patient = int(final_cpr / (fov_rate / 100) / (consent_rate / 100) / (1 - (screen_fail_rate / 100)))
    total_budget = int(dtp_volume * cost_per_patient)

    funnel_data = pd.DataFrame({
        "Metric": ["Referrals", "First Office Visits", "Consents", "Randomizations", "Projected Cost per Patient", "Total Media Budget"],
        "Value": [f"{total_referrals:,}", f"{total_fovs:,}", f"{total_consents:,}", f"{int(dtp_volume):,}", f"${cost_per_patient:,}", f"${total_budget:,}"]
    })
    st.write(funnel_data.to_html(index=False), unsafe_allow_html=True)

    # Add a funnel chart
    fig = go.Figure(go.Funnel(
        y = ["Referrals", "First Office Visits", "Consents", "Randomizations"],
        x = [total_referrals, total_fovs, total_consents, int(dtp_volume)],
        textinfo = "value+percent initial"
    ))
    fig.update_layout(title_text="Patient Funnel")
    st.plotly_chart(fig)
