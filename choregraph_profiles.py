import streamlit as st
import pandas as pd
import configparser
import os
import datetime
import snowflake.connector
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor

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
def get_ailment_data(selected_ailment, offset=0, limit=1000):
    query = f'''
        SELECT * 
        FROM PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V 
        WHERE AILMENT2_{selected_ailment} = 'Y' 
        ORDER BY DATE_OF_BIRTH_YEAR  -- Add an ORDER BY clause for consistent pagination
        LIMIT {limit} OFFSET {offset}
    '''
    cursor.execute(query)
    results = cursor.fetchall()
    if not results:
        return None
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

@st.cache_data
def get_total_count(selected_ailment):
    query = f'''
        SELECT COUNT(*) 
        FROM PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V 
        WHERE AILMENT2_{selected_ailment} = 'Y'
    '''
    cursor.execute(query)
    return cursor.fetchone()[0]

def process_chunk(df_chunk):
    # Perform your data processing on the chunk
    df_chunk['DATE_OF_BIRTH_YEAR'] = pd.to_numeric(df_chunk['DATE_OF_BIRTH_YEAR'], errors='coerce')
    df_chunk['DATE_OF_BIRTH_MONTH_'] = pd.to_numeric(df_chunk['DATE_OF_BIRTH_MONTH_'], errors='coerce')
    df_chunk['approx_age'] = CURRENT_YEAR - df_chunk['DATE_OF_BIRTH_YEAR'] - (CURRENT_MONTH - df_chunk['DATE_OF_BIRTH_MONTH_']) / 12
    df_chunk['approx_age'] = df_chunk['approx_age'].clip(upper=100)
    return df_chunk

def create_category_plot(df, prefix, title):
    columns = [col for col in df.columns if col.startswith(prefix)]
    df_category = df[columns]
    category_counts = df_category.apply(lambda x: (x == 'Y').sum()) / len(df)
    category_counts = category_counts.reset_index()
    category_counts.columns = ['Category', 'Index']
    category_counts['Category'] = category_counts['Category'].str.replace(prefix, '').str.replace('_', ' ')
    
    fig = px.bar(category_counts, x='Index', y='Category', orientation='h', title=title)
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    return fig

# Streamlit UI
st.title("Choregraph Profiles")

df = get_ailments()

# Create Ailment Selector 
ailment = st.sidebar.selectbox("Select Ailment", df['COLUMN_NAME_ADJ'])
run_button = st.sidebar.button("Run")

if run_button:
    total_count = get_total_count(ailment)
    st.write(f"Total records for this ailment: {total_count}")
    
    chunk_size = 1000
    num_chunks = (total_count + chunk_size - 1) // chunk_size
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    all_data = []
    
    for i in range(num_chunks):
        status_text.text(f"Processing chunk {i+1} of {num_chunks}...")
        df_chunk = get_ailment_data(ailment, offset=i*chunk_size, limit=chunk_size)
        if df_chunk is not None:
            all_data.append(df_chunk)
        progress_bar.progress((i + 1) / num_chunks)
    
    if not all_data:
        st.write("No data returned from the query.")
    else:
        status_text.text("Processing data...")
        
        # Use multithreading to process chunks in parallel
        with ThreadPoolExecutor() as executor:
            processed_chunks = list(executor.map(process_chunk, all_data))
        
        df_2 = pd.concat(processed_chunks, ignore_index=True)
        status_text.text("Data processed successfully.")
        
        # Create plots
        fig_age = px.histogram(df_2, x="approx_age", color="GENDER", nbins=100, 
                               histnorm='probability density', labels={'approx_age':'Age'}, 
                               title='Distribution of Age by Gender')
        
        fig_hobbies = create_category_plot(df_2, 'SURVEY_HOBBY_', 'Hobbies & Activities')
        fig_music = create_category_plot(df_2, 'SURVEY_MUSIC_', 'Musical Tastes')
        fig_owned = create_category_plot(df_2, 'SURVEY_OWN_', 'Owned Items')
        fig_occupation = create_category_plot(df_2, 'SURVEY_OCCUPATION_', 'Occupations')
        
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
