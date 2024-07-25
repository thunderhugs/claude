import pandas as pd
from datetime import datetime, timedelta

# Input data
media_plan = {
    'Date': ['Aug 24', 'Sep 24', 'Oct 24'],
    'Spend': [100000, 120000, 240000]
}

cost_per_conversion = {
    1: 150,
    2: 2000,
    3: 10000
}

conversion_delays = {
    1: 0,
    2: 14,
    3: 30
}

# Create a DataFrame from the media plan
df = pd.DataFrame(media_plan)
df['Date'] = pd.to_datetime(df['Date'], format='%b %y')

# Function to distribute spend and calculate conversions
def distribute_spend_and_calculate_conversions(row):
    start_date = row['Date']
    end_date = start_date + pd.offsets.MonthEnd(0)
    days_in_month = (end_date - start_date).days + 1
    daily_spend = row['Spend'] / days_in_month
    
    date_range = pd.date_range(start=start_date, end=end_date)
    daily_data = pd.DataFrame({'Date': date_range, 'DailySpend': daily_spend})
    
    for conv_type, cost in cost_per_conversion.items():
        daily_data[f'Conv{conv_type}'] = daily_data['DailySpend'] / cost
        daily_data[f'Conv{conv_type}Date'] = daily_data['Date'] + timedelta(days=conversion_delays[conv_type])
    
    return daily_data

# Apply the function to each row and concatenate results
result = pd.concat([distribute_spend_and_calculate_conversions(row) for _, row in df.iterrows()])

# Create a date range covering all dates in the result
all_dates = pd.date_range(start=result['Date'].min(), end=result['Date'].max())

# Create the final result DataFrame with all dates
final_result = pd.DataFrame({'Date': all_dates})

# Merge the spend data
final_result = final_result.merge(result[['Date', 'DailySpend']], on='Date', how='left')

# Function to accumulate conversions and report whole numbers
def accumulate_conversions(series):
    accumulated = 0
    result = []
    for value in series:
        accumulated += value
        whole_number = int(accumulated)
        result.append(whole_number)
        accumulated -= whole_number
    return result

# Calculate and merge the conversion data for each type
for conv_type in cost_per_conversion.keys():
    conv_data = result.groupby(f'Conv{conv_type}Date')[f'Conv{conv_type}'].sum().reset_index()
    conv_data = conv_data.rename(columns={f'Conv{conv_type}Date': 'Date'})
    final_result = final_result.merge(conv_data, on='Date', how='left')
    final_result[f'Conv{conv_type}'] = accumulate_conversions(final_result[f'Conv{conv_type}'].fillna(0))

# Fill NaN values with 0 and sort by date
final_result = final_result.fillna(0).sort_values('Date')

# Display results
pd.set_option('display.max_rows', None)  # Show all rows
print(final_result.to_string(index=False))

# Optional: Save to CSV
#final_result.to_csv('media_plan_forecast.csv', index=False)