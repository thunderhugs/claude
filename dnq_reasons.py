import snowflake.connector
import configparser
import pandas as pd
from pathlib import Path
import logging
import os
from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: Path) -> configparser.ConfigParser:
    """Load configuration from the specified path."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def connect_to_snowflake(config: configparser.ConfigParser) -> snowflake.connector.SnowflakeConnection:
    """Establish a connection to Snowflake."""
    try:
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER') or config.get("snowflake", "user"),
            password=os.getenv('SNOWFLAKE_PASSWORD') or config.get("snowflake", "password"),
            account=config.get("snowflake", "account"),
            warehouse=config.get("snowflake", "warehouse"),
            schema=config.get("snowflake", "schema"),
            role=config.get("snowflake", "role")
        )
    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        raise

def execute_snowflake_query(ctx: snowflake.connector.SnowflakeConnection, query_path: Path) -> pd.DataFrame:
    """Execute a Snowflake query from a file and return the results as a DataFrame."""
    if not query_path.exists():
        raise FileNotFoundError(f"Query file not found: {query_path}")
    
    with open(query_path, 'r') as file:
        query = file.read()
    
    logging.info(f"Executing Snowflake query from {query_path}")
    try:
        cursor = ctx.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        logging.info(f"Query executed successfully. Rows fetched: {len(df)}")
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        logging.error(f"Failed to execute Snowflake query: {e}")
        raise

def preprocess_text(text: str) -> str:
    """Preprocess the text by lowercasing, removing special characters, and extra whitespace."""
    text = str(text).lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_key_phrases(df: pd.DataFrame, column: str, top_n: int = 20) -> list:
    """Extract top key phrases from the specified column using TF-IDF."""
    vectorizer = TfidfVectorizer(ngram_range=(1, 3), stop_words='english', max_features=100)
    tfidf_matrix = vectorizer.fit_transform(df[column])
    
    feature_names = vectorizer.get_feature_names_out()
    tfidf_sum = tfidf_matrix.sum(axis=0).A1
    top_phrase_indices = tfidf_sum.argsort()[-top_n:][::-1]
    
    return [feature_names[i] for i in top_phrase_indices]

def preprocess_and_analyze(df: pd.DataFrame, column: str) -> tuple:
    """Preprocess the text data and perform initial analysis."""
    logging.info("Starting text preprocessing and analysis")
    
    # Preprocess text
    df['Processed_Text'] = df[column].fillna('').apply(preprocess_text)
    
    # Extract key phrases
    key_phrases = extract_key_phrases(df, 'Processed_Text')
    logging.info(f"Extracted key phrases: {', '.join(key_phrases)}")
    
    return df, key_phrases

def ask_ai_for_categories(df: pd.DataFrame, key_phrases: list, config: configparser.ConfigParser) -> dict:
    """Ask AI to categorize the reasons and provide counts."""
    AZURE_BROWSER_TENANT_ID = config.get("azure", "tenant_id")
    credentials = DefaultAzureCredential(
        interactive_browser_tenant_id=AZURE_BROWSER_TENANT_ID,
        exclude_cli_credential=True,
        exclude_interactive_browser_credential=False
    )

    token = credentials.get_token(config.get("azure", "token"))

    client = AzureOpenAI(
        api_key=token.token,
        azure_endpoint=config.get("azure", "endpoint"),
        api_version="2023-03-15-preview"
    )

    logging.info("Sending request to AI model for categorization")
    try:
        response = client.chat.completions.create(
            model="Qwen2-Beta-72B-Chat-vllm",
            messages=[
                {"role": "system", "content": "You're a data analyst specializing in clinical trial data. Your task is to categorize reasons for non-enrollment and provide counts for each category."},
                {"role": "user", "content": f"""
                Analyze the following data about clinical trial participants who did not qualify for the trial:

                Top Key Phrases:
                {', '.join(key_phrases)}

                Sample of Processed Reasons:
                {df['Processed_Text'].head(50).to_string()}

                Based on this information:
                1. Identify the main categories of reasons for non-enrollment.
                2. Categorize each reason in the dataset into one of these categories.
                3. Count the number of reasons in each category.
                4. Return the results as a JSON object with the following structure:
                   {{
                       "categories": [
                           {{"name": "Category Name 1", "count": 123}},
                           {{"name": "Category Name 2", "count": 456}},
                           ...
                       ]
                   }}
                
                Ensure that the categories are clear, distinct, and cover all major reasons for non-enrollment.
                """
                },
            ]
        )
        logging.info("Received response from AI model")
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        logging.error(f"Error in AI request: {e}")
        raise

def main() -> None:
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / 'config.ini'
    
    try:
        config = load_config(config_path)
        logging.info("Configuration loaded successfully")

        with connect_to_snowflake(config) as ctx:
            df_snowflake = execute_snowflake_query(ctx, script_dir / 'dnq_reasons.sql')

        # Preprocess and analyze the data
        processed_df, key_phrases = preprocess_and_analyze(df_snowflake, 'NON_ENRL_RSN')

        # Ask AI to categorize the reasons and provide counts
        categorization_result = ask_ai_for_categories(processed_df, key_phrases, config)

        # Create a DataFrame from the AI's categorization
        categories_df = pd.DataFrame(categorization_result['categories'])
        
        # Sort the DataFrame by count
