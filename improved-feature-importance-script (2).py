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

def connect_to_snowflake(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    try:
        ctx = snowflake.connector.connect(
            user=config.get("snowflake", "user"),
            password=config.get("snowflake", "password"),
            account=config.get("snowflake", "account"),
            warehouse=config.get("snowflake", "warehouse"),
            schema=config.get("snowflake", "schema"),
            role=config.get("snowflake", "role"),
        )
        print("Connection successful")
        return ctx
    except Exception as e:
        print("Error:", e)
        return None

def execute_query(ctx, query_path):
    with open(query_path, 'r') as file:
        query = file.read()
    
    cursor = ctx.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    
    return pd.DataFrame(results, columns=column_names)

def preprocess_data(df, datadic_path, target):
    datadic = pd.read_csv(datadic_path)[['Asset field name', 'Name']]
    datadic['Asset field name'] = datadic['Asset field name'].str.lower()
    df.columns = df.columns.str.lower()
    
    name_dict = datadic.set_index('Asset field name')['Name'].to_dict()
    df.rename(columns=name_dict, inplace=True)
    
    # Create a binary target variable
    df[target] = (df[target] == 'Y').astype(int)
    
    y = df[target]
    X = df.drop(target, axis=1)
    
    # Handle categorical variables
    for col in X.select_dtypes(include=['object']).columns:
        X[col] = LabelEncoder().fit_transform(X[col].astype(str))
    
    return X, y

def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y, random_state=42)
    
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    
    return clf, X_test, y_test

def get_feature_importances(clf, X):
    importances = clf.feature_importances_
    feature_importances = pd.DataFrame({'feature': X.columns, 'importance': importances})
    return feature_importances.sort_values('importance', ascending=False)

def plot_feature_importances(feature_importances, top_n=10):
    top_features = feature_importances.head(top_n)
    
    plt.figure(figsize=(10, 8))
    sns.barplot(x='importance', y='feature', data=top_features, orient='h', color='blue')
    plt.title(f'Top {top_n} Feature Importances')
    plt.xlabel('Importance')
    plt.ylabel('Feature')
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
    
    clf, X_test, y_test = train_model(X, y)
    feature_importances = get_feature_importances(clf, X)
    
    print(feature_importances)
    plot_feature_importances(feature_importances)
    
    # Print additional information
    print(f"Target variable distribution:\n{y.value_counts(normalize=True)}")
    print(f"Number of features: {X.shape[1]}")
    print(f"Sample size: {len(y)}")

if __name__ == "__main__":
    main()
