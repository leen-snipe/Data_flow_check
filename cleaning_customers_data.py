import pandas as pd
import os
import requests

folder_path = "C:\\Users\\SnipeGuest\\jobs3"
output_csv_path = 'C:\\Users\\SnipeGuest\\Data_flow_check\\aggregated_results4.csv'
api_url = 'https://agrilive.snipe.ps/api/sendreport/'
api_token = 'da4699890b923a33529409ae3cd2bc65092247b6'

column_name_mapping = {
    'Main Qunatity Flowrate (M³/h)': 'mq',
    'F1 Flowrate(L/h)': 'f1',
    'F2 Flowrate(L/h)': 'f2',
    'F3 Flowrate(L/h)': 'f3',
    'F4 Flowrate(L/h)': 'f4',
    'F5 Flowrate(L/h)': 'f5',
    'F6 Flowrate(L/h)': 'f6'
}

picked_columns = [
    'Main Qunatity Flowrate (M³/h)',
    'F1 Flowrate(L/h)',
    'F2 Flowrate(L/h)',
    'F3 Flowrate(L/h)',
    'F4 Flowrate(L/h)',
    'F5 Flowrate(L/h)',
    'F6 Flowrate(L/h)'
]

results = []

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    if file_name.endswith('.xlsx'):
        try:
            df = pd.read_excel(file_path)
            df.columns = df.columns.str.strip()
            
            # Check for required columns
            if 'job_id' not in df.columns or 'device' not in df.columns:
                print(f"Warning: 'job_id' or 'device' column not found in {file_name}. Skipping this file.")
                continue
            
            # Extract job_id and device_id
            job_id = df['job_id'].dropna().iloc[0] if not df['job_id'].dropna().empty else None
            device_id = df['device'].dropna().iloc[0] if not df['device'].dropna().empty else None
            
            # Check if the job_id and device_id are valid
            if job_id is None or device_id is None:
                print(f"Warning: Missing 'job_id' or 'device_id' in {file_name}. Skipping this file.")
                continue
            
            # Include additional columns in the result_row
            date = df['date'].dropna().iloc[0] if 'date' in df.columns else None
            start_time_job_id = df['start_time_job_id'].dropna().iloc[0] if 'start_time_job_id' in df.columns else None
            starttime = df['starttime'].dropna().iloc[0] if 'starttime' in df.columns else None
            
            if date is not None:
                date = pd.to_datetime(date).strftime('%Y-%m-%d %H:%M:%S')


            df_selected_columns = df[picked_columns]
            df_selected_columns = df_selected_columns.apply(pd.to_numeric, errors='coerce')
            threshold = 0
            stats = {}
            
            # Calculate stats for each selected column
            for column in df_selected_columns.columns:
                short_name = column_name_mapping.get(column, column)
                if column == 'Main Qunatity Flowrate (M³/h)':
                    non_zero_values = df_selected_columns[column]
                else:
                    non_zero_values = df_selected_columns[column][df_selected_columns[column] > threshold]
                
                if non_zero_values.empty:
                    stats[short_name] = {'Mode': None, 'Median': None, 'Mean': None}
                else:
                    mode_value = non_zero_values.mode().iloc[0] if not non_zero_values.mode().empty else None
                    median_value = non_zero_values.median()
                    mean_value = non_zero_values.mean()
                    
                    # Round the values
                    if mode_value is not None:
                        mode_value = round(mode_value, 2)
                    if median_value is not None:
                        median_value = round(median_value, 2)
                    if mean_value is not None:
                        mean_value = round(mean_value, 2)
                    
                    stats[short_name] = {'Mode': mode_value, 'Median': median_value, 'Mean': mean_value}
            
            # Add the necessary columns to the result row
            result_row = {
                'job_id': job_id,
                'device_id': device_id,
                'date': date,
                'start_time_job_id': start_time_job_id,
                'starttime': starttime
            }
            
            # Add stats to the result row
            for short_name, stat_values in stats.items():
                result_row[f'{short_name}_mode'] = stat_values['Mode']
                result_row[f'{short_name}_median'] = stat_values['Median']
                result_row[f'{short_name}_mean'] = stat_values['Mean']
            
            results.append(result_row)
            print(f"Processed file: {file_name}")
        
        except Exception as e:
            print(f"Error processing file {file_name}: {str(e)}")

# Convert results to DataFrame and save to CSV
results_df = pd.DataFrame(results)
print(results_df)
results_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

# Send the CSV file to the API endpoint
files = {'file': open(output_csv_path, 'rb')}
headers = {
    'Authorization': f'Token {api_token}'
}
response = requests.post(api_url, files=files, headers=headers)
print(response.status_code)
print(response.json())
