import streamlit as st  
import pandas as pd
import configparser
import os
import datetime
import snowflake.connector
import matplotlib.pyplot as plt
import plotly.express as px

current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# Path to the config file
config_path = os.path.join(script_dir, 'config.ini')

# Read the config file
config = configparser.ConfigParser()
config.read(config_path)

# Snowflake connection 
ctx = snowflake.connector.connect(
    user = config.get('snowflake','user'),
    password = config.get('snowflake','password'),
    account = config.get('snowflake','account'),
    warehouse = config.get('snowflake', 'warehouse'),
    schema = config.get('snowflake', 'schema'),
    role = config.get('snowflake', 'role'))

cursor = ctx.cursor()

# SQL query to retrieve column names
query_1 = '''SELECT 
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
    cursor.execute(query_1)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    return df

df = get_ailments()

# Create Ailment Selector 
ailment = st.sidebar.selectbox("Select Ailment", df['COLUMN_NAME_ADJ'])
run_button = st.sidebar.button("run")

selected_ailment = ailment

@st.cache_data()
def get_ailment_data(selected_ailment):
    query_2 = f'''SELECT * from PROD_US9_PAR_RPR.PRSP_LGM_RPT_USER_V WHERE AILMENT2_{selected_ailment} = 'Y' limit 1000'''
    cursor.execute(query_2)
    results = cursor.fetchall()
    if not results:
        return None
    column_names = [column[0] for column in cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    return df

if run_button:
    df_2 = get_ailment_data(selected_ailment)
    if df_2 is None:
        st.write("No data returned from the query.")
    else:
        st.write("All good in the hood")

# Remove leading and trailing whitespace from column names and convert to lowercase
df.columns = df.columns.str.strip().str.lower()

# Convert 'DATE_OF_BIRTH_YEAR' column to numeric values
df_2['DATE_OF_BIRTH_YEAR'] = pd.to_numeric(df_2['DATE_OF_BIRTH_YEAR'], errors='coerce')
df_2['DATE_OF_BIRTH_MONTH_'] = pd.to_numeric(df_2['DATE_OF_BIRTH_MONTH_'], errors='coerce')


# Check if 'DATE_OF_BIRTH_YEAR' and 'DATE_OF_BIRTH_MONTH_' columns exist in df_2
if 'DATE_OF_BIRTH_YEAR' not in df_2.columns or 'DATE_OF_BIRTH_MONTH_' not in df_2.columns:
    st.write("DATE_OF_BIRTH_YEAR or DATE_OF_BIRTH_MONTH_ column not found in df_2.")
else:
    # Check if 'DATE_OF_BIRTH_YEAR' and 'DATE_OF_BIRTH_MONTH_' columns contain non-numeric values
    if not pd.api.types.is_numeric_dtype(df_2['DATE_OF_BIRTH_YEAR']) or not pd.api.types.is_numeric_dtype(df_2['DATE_OF_BIRTH_MONTH_']):
        st.write("DATE_OF_BIRTH_YEAR or DATE_OF_BIRTH_MONTH_ column contains non-numeric values.")
    else:
        # Check if 'DATE_OF_BIRTH_YEAR' and 'DATE_OF_BIRTH_MONTH_' columns contain NaN values
        if df_2['DATE_OF_BIRTH_YEAR'].isna().sum() > 0 or df_2['DATE_OF_BIRTH_MONTH_'].isna().sum() > 0:
            st.write("DATE_OF_BIRTH_YEAR or DATE_OF_BIRTH_MONTH_ column contains NaN values.")
        else:
            # If there are no issues, calculate 'approx_age' using year and month
            df_2['approx_age'] = current_year - df_2['DATE_OF_BIRTH_YEAR'] - (current_month - df_2['DATE_OF_BIRTH_MONTH_']) / 12

# Cap 'approx_age' at 75
df_2['approx_age'] = df_2['approx_age'].clip(upper=100)


# Create a histogram with Plotly
fig = px.histogram(df_2, x="approx_age", color = "GENDER", nbins=100, histnorm= 'probability density', labels={'approx_age':'Age'}, title='Distribution of Age, Gender')

# Use Streamlit's built-in function to display the plot
st.plotly_chart(fig)

# Filter out columns that start with "SURVEY_HOBBY_"
hobby_columns = [col for col in df_2.columns if col.startswith('SURVEY_HOBBY_')]

# Create a new DataFrame with only the hobby columns
df_hobbies = df_2[hobby_columns]

# Calculate the count of 'Y' values in each column
hobby_counts = df_hobbies.apply(lambda x: (x == 'Y').sum())

# Calculate the proportion of 'Y' values relative to the total number of records
hobby_counts = hobby_counts / len(df_2)

# Convert the Series to a DataFrame and reset the index
hobby_counts = hobby_counts.reset_index()

# Rename the columns
hobby_counts.columns = ['Hobby', 'Index']

# Remove the text "SURVEY_HOBBY_" and replace underscores with spaces
hobby_counts['Hobby'] = hobby_counts['Hobby'].str.replace('SURVEY_HOBBY_', '').str.replace('_', ' ')

# Create a horizontal bar chart with Plotly
fig_hobby = px.bar(hobby_counts, x='Index', y='Hobby', orientation='h', title='Hobbies & Activities')

# Sort the bars by 'Index' in descending order
fig_hobby.update_layout(yaxis={'categoryorder':'total ascending'})

# Use Streamlit's built-in function to display the plot
st.plotly_chart(fig_hobby)

#-----------MUSIC----------------

# Filter out columns that start with "SURVEY_HOBBY_"
music_columns = [col for col in df_2.columns if col.startswith('SURVEY_MUSIC_')]

# Create a new DataFrame with only the hobby columns
df_music = df_2[music_columns]

# Calculate the count of 'Y' values in each column
music_counts = df_music.apply(lambda x: (x == 'Y').sum())

# Calculate the proportion of 'Y' values relative to the total number of records
music_counts = music_counts / len(df_2)

# Convert the Series to a DataFrame and reset the index
music_counts = music_counts.reset_index()

# Rename the columns
music_counts.columns = ['Music Genre', 'Index']

# Remove the text "SURVEY_MUSIC_" and replace underscores with spaces
music_counts['Music Genre'] = music_counts['Music Genre'].str.replace('SURVEY_MUSIC_', '').str.replace('_', ' ')

# Create a horizontal bar chart with Plotly
fig_music = px.bar(music_counts, x='Index', y='Music Genre', orientation='h', title='Musical Tastes')

# Sort the bars by 'Index' in descending order
fig_music.update_layout(yaxis={'categoryorder':'total ascending'})

# Use Streamlit's built-in function to display the plot
st.plotly_chart(fig_music)

#-----------SURVEY_OWNS----------------

# Filter out columns that start with "SURVEY_OWNS_"
owns_columns = [col for col in df_2.columns if col.startswith('SURVEY_OWN_')]

# Create a new DataFrame with only the owns columns
df_owns = df_2[owns_columns]

# Calculate the count of 'Y' values in each column
owns_counts = df_owns.apply(lambda x: (x == 'Y').sum())

# Calculate the proportion of 'Y' values relative to the total number of records
owns_counts = owns_counts / len(df_2)

# Convert the Series to a DataFrame and reset the index
owns_counts = owns_counts.reset_index()

# Rename the columns
owns_counts.columns = ['Owned Item', 'Index']

# Remove the text "SURVEY_OWNS_" and replace underscores with spaces
owns_counts['Owned Item'] = owns_counts['Owned Item'].str.replace('SURVEY_OWN_', '').str.replace('_', ' ')

# Create a horizontal bar chart with Plotly
fig_owns = px.bar(owns_counts, x='Index', y='Owned Item', orientation='h', title='Owned Items')

# Sort the bars by 'Index' in descending order
fig_owns.update_layout(yaxis={'categoryorder':'total ascending'})

# Use Streamlit's built-in function to display the plot
st.plotly_chart(fig_owns)

#-----------SURVEY_OCCUPATION----------------

# Filter out columns that start with "SURVEY_OCCUPATION_"
occupation_columns = [col for col in df_2.columns if col.startswith('SURVEY_OCCUPATION_')]

# Create a new DataFrame with only the occupation columns
df_occupation = df_2[occupation_columns]

# Calculate the count of 'Y' values in each column
occupation_counts = df_occupation.apply(lambda x: (x == 'Y').sum())

# Calculate the proportion of 'Y' values relative to the total number of records
occupation_counts = occupation_counts / len(df_2)

# Convert the Series to a DataFrame and reset the index
occupation_counts = occupation_counts.reset_index()

# Rename the columns
occupation_counts.columns = ['Occupation', 'Index']

# Remove the text "SURVEY_OCCUPATION_" and replace underscores with spaces
occupation_counts['Occupation'] = occupation_counts['Occupation'].str.replace('SURVEY_OCCUPATION_', '').str.replace('_', ' ')

# Create a horizontal bar chart with Plotly
fig_occupation = px.bar(occupation_counts, x='Index', y='Occupation', orientation='h', title='Occupations')

# Sort the bars by 'Index' in descending order
fig_occupation.update_layout(yaxis={'categoryorder':'total ascending'})

# Use Streamlit's built-in function to display the plot
st.plotly_chart(fig_occupation)