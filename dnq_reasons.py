import snowflake.connector
import configparser
import pandas as pd
from pathlib import Path
import logging
import requests
from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential

def load_config(config_path: str) -> configparser.ConfigParser:
    """Load configuration from the specified path."""
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def connect_to_snowflake(config: configparser.ConfigParser) -> snowflake.connector.SnowflakeConnection:
    """Establish a connection to Snowflake."""
    return snowflake.connector.connect(
        user=config.get("snowflake", "user"),
        password=config.get("snowflake", "password"),
        account=config.get("snowflake", "account"),
        warehouse=config.get("snowflake", "warehouse"),
        schema=config.get("snowflake", "schema"),
        role=config.get("snowflake", "role")
    )

def execute_snowflake_query(ctx: snowflake.connector.SnowflakeConnection, query_path: str) -> pd.DataFrame:
    """Execute a Snowflake query from a file and return the results as a DataFrame."""
    with open(query_path, 'r') as file:
        query = file.read()
    
    cursor = ctx.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    return pd.DataFrame(results, columns=column_names)

def ask_ai(df, config):
    AZURE_BROWSER_TENANT_ID = config.get("azure", "tenant_id")
    credentials = DefaultAzureCredential(interactive_browser_tenant_id=AZURE_BROWSER_TENANT_ID,
                                        exclude_cli_credential=True, exclude_interactive_browser_credential=False)

    token = credentials.get_token(config.get("azure", "token"))

    client = AzureOpenAI(api_key=token.token,azure_endpoint=config.get("azure", "endpoint"),api_version="2023-03-15-preview")

    response = client.chat.completions.create(
        model="Qwen2-Beta-72B-Chat-vllm",
        messages=[
            {"role": "system", "content": "You're a data analyst. You process data requests with enthusiam and accuracy."},
            {"role": "user", "content": f"The below table contains a list of clinical trial participants who did not qualify for the trial. Please bucket these reasons and provide counts these reasons.
             Example Output Format:
             PARTICIPANT_STATUS_STAGE | Non Enrolment Reason | Count
             Pre Review - Failed | No UC diagniosis | 2
             Contacted Not Suitable | Stelera Usage | 1
             "},
        ]
    )

    # Extract the content from the response
    content = response.choices[0].message.content

    return content

def main():
    script_dir = Path(__file__).parent
    config = load_config(script_dir / 'config.ini')
    
    # Connect to Snowflake and fetch data
    with connect_to_snowflake(config) as ctx:
        df_snowflake = execute_snowflake_query(ctx, script_dir / 'dnq_reasons.sql')
    
    logging.info(f"Snowflake data loaded: {df_snowflake.columns}")

    content = ask_ai(df_snowflake['NON_ENRL_RSN'], config)
    print(content)

if __name__ == "__main__":
    main()