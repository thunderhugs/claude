import streamlit as st
import pandas as pd
import configparser
import os
import datetime
import snowflake.connector
import plotly.express as px

st.set_page_config(layout="wide", initial_sidebar_state="expanded", page_icon=None, page_title=None)

# Constants
CURRENT_YEAR = datetime.datetime.now().year
CURRENT_MONTH = datetime.datetime.now().month
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, 'config.ini')

# Read the config file
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
    user = config.get("snowflake", "user", fallback=os.getenv("SNOWFLAKE_USER")),
    password = config.get("snowflake", "password", fallback=os.getenv("SNOWFLAKE_PASSWORD")),
    account= 'iqviaidporg-prdbus_reporting',
    warehouse = config.get("snowflake", "warehouse", fallback=os.getenv("SNOWFLAKE_WAREHOUSE")),
    schema = config.get("snowflake", "schema", fallback=os.getenv("SNOWFLAKE_SCHEMA")),
    role = config.get("snowflake", "role", fallback=os.getenv("SNOWFLAKE_ROLE")))

ctx = get_snowflake_connection()
cursor = ctx.cursor()

# SQL queries
AILMENT_QUERY = '''
    SELECT 
        column_name,
        REGEXP_REPLACE(column_name, 'AILMENT2_', '') AS column_name_adj
    FROM information_schema.columns
    WHERE 
        table_name = 'PRSP_LGM_RPT_USER_V'
        AND table_schema = 'PROD_US9_PAR_RPR'
        AND lower(column_name) LIKE 'ailment%'
'''

@st.cache_data
def get_ailments():
    cursor.execute(AILMENT_QUERY)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

@st.cache_data
def get_age_distribution(selected_ailment, sample_size=100000):
    query = f'''
        SELECT 
            GENDER,
            AVG(({CURRENT_YEAR} - DATE_OF_BIRTH_YEAR) - ({CURRENT_MONTH} - DATE_OF_BIRTH_MONTH_) / 12) as approx_age,
            COUNT(*) as count
        FROM 
            (SELECT DATE_OF_BIRTH_YEAR, DATE_OF_BIRTH_MONTH_, GENDER
             FROM PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V 
             WHERE AILMENT2_{selected_ailment} = 'Y'
             ORDER BY RANDOM()
             LIMIT {sample_size}
            )
        GROUP BY GENDER
        ORDER BY GENDER
    '''
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0].lower() for column in cursor.description]  # convert column names to lowercase
    df = pd.DataFrame(results, columns=column_names)
    return df

@st.cache_data
def get_category_distribution(selected_ailment, category_prefix, sample_size=100000):
    columns = ", ".join([f"SUM(CASE WHEN {col[0]} = 'Y' THEN 1 ELSE 0 END) as {col[0]}" 
                         for col in cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'PRSP_LGM_RPT_USER_V' AND column_name LIKE '{category_prefix}%'").fetchall()])
    
    query = f'''
        SELECT {columns}
        FROM 
            (SELECT *
             FROM PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V 
             WHERE AILMENT2_{selected_ailment} = 'Y'
             ORDER BY RANDOM()
             LIMIT {sample_size}
            )
    '''
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    
    # Normalize the results
    total = df.iloc[0].sum()
    df = df.apply(lambda x: x / total)
    
    df = df.melt(var_name='Category', value_name='Proportion')
    df['Category'] = df['Category'].str.replace(category_prefix, '').str.replace('_', ' ')

    # Exclude rows with Proportion value of 0
    df = df[df['Proportion'] != 0]

    return df

@st.cache_data
def get_total_count(selected_ailment):
    query = f'''
        SELECT COUNT(*) 
        FROM PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V 
        WHERE AILMENT2_{selected_ailment} = 'Y'
    '''
    cursor.execute(query)
    return cursor.fetchone()[0]

# Streamlit UI
st.title("Choregraph Profiles")

df = get_ailments()

# Create Ailment Selector 
ailment = st.sidebar.selectbox("Select Ailment", df['COLUMN_NAME_ADJ'])
sample_size = st.sidebar.number_input('Sample Size', min_value=1, max_value=1000000, value=10000, step=1000)
run_button = st.sidebar.button("Run")

if run_button:
    total_count = get_total_count(ailment)
    st.write(f"Total records for this ailment: {total_count}")
    
    with st.spinner("Fetching and processing data..."):
        # Age distribution
        df_age = get_age_distribution(ailment)
        fig_age = px.bar(df_age, x="gender", y="count", color="gender",
                 labels={'count':'Count', 'gender':'Gender'},
                 title=f'Distribution of Gender (Avg Age: {df_age["approx_age"].mean():.2f})')
        
        fig_hobbies = px.bar(get_category_distribution(ailment, 'SURVEY_HOBBY_', sample_size=sample_size), 
                     x='Proportion', y='Category', orientation='h', title='Hobbies & Activities',
                     labels={'Proportion':'', 'Category':''})

        fig_music = px.bar(get_category_distribution(ailment, 'SURVEY_MUSIC_', sample_size=sample_size), 
                        x='Proportion', y='Category', orientation='h', title='Musical Tastes',
                        labels={'Proportion':'', 'Category':''})

        fig_owned = px.bar(get_category_distribution(ailment, 'SURVEY_OWN_', sample_size=sample_size), 
                        x='Proportion', y='Category', orientation='h', title='Owned Items',
                        labels={'Proportion':'', 'Category':''})

        fig_occupation = px.bar(get_category_distribution(ailment, 'SURVEY_OCCUPATION_', sample_size=sample_size), 
                                x='Proportion', y='Category', orientation='h', title='Occupations',
                                labels={'Proportion':'', 'Category':''})

        fig_collectables = px.bar(get_category_distribution(ailment, 'SURVEY_COLLECTIBLES_', sample_size=sample_size), 
                                x='Proportion', y='Category', orientation='h', title='Collectables',
                                labels={'Proportion':'', 'Category':''})
        
        fig_credit_cards = px.bar(get_category_distribution(ailment, 'SURVEY_CREDIT_CARDS_', sample_size=sample_size), 
                                x='Proportion', y='Category', orientation='h', title='Credit Cards',
                                labels={'Proportion':'', 'Category':''})
        
        fig_diet_concerns = px.bar(get_category_distribution(ailment, 'SURVEY_DIET_CONCERNS_', sample_size=sample_size), 
                                x='Proportion', y='Category', orientation='h', title='Diet Concerns',
                                labels={'Proportion':'', 'Category':''})
        
        fig_mail_order = px.bar(get_category_distribution(ailment, 'SURVEY_MAIL_ORDER_', sample_size=sample_size), 
                                x='Proportion', y='Category', orientation='h', title='Mail Order',
                                labels={'Proportion':'', 'Category':''})
        fig_investments = px.bar(get_category_distribution(ailment, 'SURVEY_INVESTMENTS_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Investments',
                                labels={'Proportion':'', 'Category':''})
        
        fig_reading = px.bar(get_category_distribution(ailment, 'SURVEY_READING_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Reading Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_donor = px.bar(get_category_distribution(ailment, 'SURVEY_DONOR_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Donor Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_sporting = px.bar(get_category_distribution(ailment, 'SURVEY_SPORTING_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Sporting Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_travel = px.bar(get_category_distribution(ailment, 'SURVEY_TRAVEL_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Travel Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_electronics = px.bar(get_category_distribution(ailment, 'SURVEY_ELECTRONICS_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Electronics Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_purchase = px.bar(get_category_distribution(ailment, 'SURVEY_PURCHASE_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Purchase Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_group = px.bar(get_category_distribution(ailment, 'SURVEY_GROUP_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Group Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_retail = px.bar(get_category_distribution(ailment, 'BUYER_RETAIL_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Retail Preferences',
                                labels={'Proportion':'', 'Category':''})
        
        fig_treatment = px.bar(get_category_distribution(ailment, 'TREATMENT2_', sample_size=sample_size),
                                x='Proportion', y='Category', orientation='h', title='Treatment Preferences',
                                labels={'Proportion':'', 'Category':''})
    
    # Display plots in a grid
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.plotly_chart(fig_age, use_container_width=True)
        st.plotly_chart(fig_treatment, use_container_width=True)
        st.plotly_chart(fig_hobbies, use_container_width=True)
        st.plotly_chart(fig_credit_cards, use_container_width=True)
        st.plotly_chart(fig_investments, use_container_width=True)
        st.plotly_chart(fig_sporting, use_container_width=True)
        st.plotly_chart(fig_purchase, use_container_width=True)
    
    with col2:
        st.plotly_chart(fig_music, use_container_width=True)
        st.plotly_chart(fig_owned, use_container_width=True)
        st.plotly_chart(fig_diet_concerns, use_container_width=True)
        st.plotly_chart(fig_reading, use_container_width=True)
        st.plotly_chart(fig_travel, use_container_width=True)
        st.plotly_chart(fig_group, use_container_width=True)
    
    with col3:
        st.plotly_chart(fig_occupation, use_container_width=True)
        st.plotly_chart(fig_collectables, use_container_width=True)
        st.plotly_chart(fig_mail_order, use_container_width=True)
        st.plotly_chart(fig_donor, use_container_width=True)
        st.plotly_chart(fig_electronics, use_container_width=True)
        st.plotly_chart(fig_retail, use_container_width=True)

# Close the Snowflake connection when the app is done
st.cache_resource.clear()
