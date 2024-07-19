import snowflake.connector
import configparser
import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns

# ... (previous functions remain unchanged)

def analyze_age_distribution(df, target):
    # Ensure 'Age' column exists
    if 'Age' not in df.columns:
        print("Error: 'Age' column not found in the dataset.")
        return None

    # Create age groups
    df['Age_Group'] = pd.cut(df['Age'], bins=[0, 18, 30, 45, 60, 100], labels=['0-18', '19-30', '31-45', '46-60', '60+'])

    # Calculate the target rate for each age group
    age_distribution = df.groupby('Age_Group')[target].mean().reset_index()
    age_distribution.columns = ['Age_Group', 'Target_Rate']

    # Calculate the overall target rate
    overall_rate = df[target].mean()

    # Calculate the index (ratio of age group rate to overall rate)
    age_distribution['Index'] = age_distribution['Target_Rate'] / overall_rate

    return age_distribution

def plot_age_distribution(age_distribution, target):
    plt.figure(figsize=(12, 6))
    sns.barplot(x='Age_Group', y='Index', data=age_distribution)
    plt.title(f'Age Group Index for {target}')
    plt.xlabel('Age Group')
    plt.ylabel('Index (1.0 = Average)')
    plt.axhline(y=1, color='r', linestyle='--')
    plt.tight_layout()
    plt.show()

def main():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')
    query_path = os.path.join(script_dir, 'query_1.sql')
    datadic_path = os.path.join(script_dir, 'DataDic.csv')
    
    ctx = connect_to_snowflake(config_path)
    if ctx is None:
        return
    
    df = execute_query(ctx, query_path)
    target = 'Ailment2 - Acne'
    X, y = preprocess_data(df, datadic_path, target)
    
    # Perform age distribution analysis
    age_distribution = analyze_age_distribution(df, target)
    if age_distribution is not None:
        print("Age Distribution Analysis:")
        print(age_distribution)
        plot_age_distribution(age_distribution, target)
    
    clf, X_test, y_test = train_model(X, y)
    feature_importances = get_feature_importances(clf, X)
    
    print("Feature Importances:")
    print(feature_importances)
    plot_feature_importances(feature_importances)
    
    # Print additional information
    print(f"Target variable distribution:\n{y.value_counts(normalize=True)}")
    print(f"Number of features: {X.shape[1]}")
    print(f"Sample size: {len(y)}")

if __name__ == "__main__":
    main()
