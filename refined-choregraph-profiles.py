import streamlit as st
import pandas as pd
import configparser
import os
import datetime
import snowflake.connector
import plotly.express as px

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
        user=config.get('snowflake', 'user'),
        password=config.get('snowflake', 'password'),
        account=config.get('snowflake', 'account'),
        warehouse=config.get('snowflake', 'warehouse'),
        schema=config.get('snowflake', 'schema'),
        role=config.get('snowflake', 'role')
    )

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
             SAMPLE ({sample_size} ROWS)
            )
        GROUP BY GENDER
        ORDER BY GENDER
    '''
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

@st.cache_data
def get_category_distribution(selected_ailment, category_prefix, sample_size=100000):
    columns = ", ".join([f"SUM(CASE WHEN {col} = 'Y' THEN 1 ELSE 0 END) as {col}" 
                         for col in cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'PRSP_LGM_RPT_USER_V' AND column_name LIKE '{category_prefix}%'").fetchall()])
    
    query = f'''
        SELECT {columns}
        FROM 
            (SELECT *
             FROM PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V 
             WHERE AILMENT2_{selected_ailment} = 'Y'
             SAMPLE ({sample_size} ROWS)
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
run_button = st.sidebar.button("Run")

if run_button:
    total_count = get_total_count(ailment)
    st.write(f"Total records for this ailment: {total_count}")
    
    with st.spinner("Fetching and processing data..."):
        # Age distribution
        df_age = get_age_distribution(ailment)
        fig_age = px.bar(df_age, x="GENDER", y="count", color="GENDER",
                         labels={'count':'Count', 'GENDER':'Gender'},
                         title=f'Distribution of Gender (Avg Age: {df_age["approx_age"].mean():.2f})')
        
        # Category plots
        fig_hobbies = px.bar(get_category_distribution(ailment, 'SURVEY_HOBBY_'), 
                             x='Proportion', y='Category', orientation='h', title='Hobbies & Activities')
        fig_music = px.bar(get_category_distribution(ailment, 'SURVEY_MUSIC_'), 
                           x='Proportion', y='Category', orientation='h', title='Musical Tastes')
        fig_owned = px.bar(get_category_distribution(ailment, 'SURVEY_OWN_'), 
                           x='Proportion', y='Category', orientation='h', title='Owned Items')
        fig_occupation = px.bar(get_category_distribution(ailment, 'SURVEY_OCCUPATION_'), 
                                x='Proportion', y='Category', orientation='h', title='Occupations')
    
    # Display plots in a grid
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.plotly_chart(fig_age, use_container_width=True)
        st.plotly_chart(fig_hobbies, use_container_width=True)
    
    with col2:
        st.plotly_chart(fig_music, use_container_width=True)
        st.plotly_chart(fig_owned, use_container_width=True)
    
    with col3:
        st.plotly_chart(fig_occupation, use_container_width=True)

# Close the Snowflake connection when the app is done
st.cache_resource.clear()
