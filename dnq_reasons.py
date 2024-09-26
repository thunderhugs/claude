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
import io

# ... (keep all the previous imports and functions up to ask_ai_for_categories)

def ask_ai_for_categories(df: pd.DataFrame, key_phrases: list, config: configparser.ConfigParser) -> pd.DataFrame:
    """Ask AI to categorize the reasons and provide counts."""
    # ... (keep the existing code up to the client.chat.completions.create call)

    logging.info("Sending request to AI model for categorization")
    try:
        response = client.chat.completions.create(
            model="Qwen2-72B-Instruct-vllm",
            messages=[
                {"role": "system", "content": "You're a data analyst specializing in clinical trial data. Your task is to categorize reasons for non-enrollment and provide counts for each category."},
                {"role": "user", "content": f"""
                Analyze the following data about clinical trial participants who did not qualify for the trial:
                
                Data Table: 
                {df.to_string()}

                Top Key Phrases:
                {', '.join(key_phrases)}

                Sample of Processed Reasons:
                {df['Processed_Text'].head(50).to_string()}

                Based on this information:
                1. Identify the main categories of reasons for non-enrollment.
                2. Categorize each reason in the dataset into one of these categories.
                3. Return a data table with the following columns: NON_ENRL_RSN, Category, RANDOM_ID
                4. Ensure that the 'NON_ENRL_RSN' and 'RANDOM_ID' columns contain the original data.
                5. The 'Category' column should contain the derived category for each reason.
                6. Use pipe (|) as the delimiter for the data table.
                7. Include a header row in the output.

                Output Template:

                NON_ENRL_RSN|Category|RANDOM_ID
                original reason|derived category|original random id
                ...

                Ensure that the categories are clear, distinct, and cover all major reasons for non-enrollment.
                """
                },
            ]
        )
        logging.info("Received response from AI model")
        
        # Parse the response content
        raw_response_content = response.choices[0].message.content
        logging.debug(f"Raw response content: {raw_response_content}")

        # Convert the raw response content into a DataFrame
        try:
            result_df = pd.read_csv(io.StringIO(raw_response_content), sep='|')
            logging.info(f"Parsed AI response into DataFrame with shape: {result_df.shape}")
            return result_df
        except Exception as parse_err:
            logging.error(f"Error parsing response content: {parse_err}")
            logging.error(f"Failed to parse response content: {raw_response_content}")
            raise

    except Exception as e:
        logging.error(f"Error in AI request: {e}")
        raise

def merge_categories_with_original_data(original_df: pd.DataFrame, categories_df: pd.DataFrame) -> pd.DataFrame:
    """Merge the AI-generated categories with the original data."""
    logging.info("Merging AI-generated categories with original data")
    
    # Ensure the 'RANDOM_ID' column is present in both DataFrames
    if 'RANDOM_ID' not in original_df.columns or 'RANDOM_ID' not in categories_df.columns:
        raise ValueError("Both DataFrames must contain a 'RANDOM_ID' column for merging")

    # Merge the DataFrames based on the 'RANDOM_ID' column
    merged_df = pd.merge(original_df, categories_df[['RANDOM_ID', 'Category']], on='RANDOM_ID', how='left')
    
    logging.info(f"Merged DataFrame shape: {merged_df.shape}")
    return merged_df

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

        # Ask AI to categorize the reasons
        categorization_result = ask_ai_for_categories(processed_df, key_phrases, config)

        # Merge the AI-generated categories with the original data
        final_df = merge_categories_with_original_data(df_snowflake, categorization_result)

        # Display the results
        print("Original data with AI-generated categories:")
        print(final_df.to_string(index=False))

        # Optionally, save the results to a CSV file
        output_path = script_dir / 'non_enrollment_reasons_with_categories.csv'
        final_df.to_csv(output_path, index=False)
        logging.info(f"Results saved to {output_path}")

        logging.info("Analysis completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
