import pandas as pd # type: ignore

df = pd.read_excel('C:\\Users\\SnipeGuest\\Data_flow_check\\dataset.xlsx')

print(df)

df.head(10)

# List of picked columns to process
picked_columns = [
    'Main Qunatity Flowrate (M³/h)',
    'F1 Flowrate(L/h)',
    'F2 Flowrate(L/h)',
    'F3 Flowrate(L/h)',
    'F4 Flowrate(L/h)',
    'F5 Flowrate(L/h)',
    'F6 Flowrate(L/h)'
]

# Select the relevant columns
df_selected_columns = df[picked_columns]

print(df_selected_columns)

df_selected_columns.head(5)

# Threshold to filter out zero values for all columns except 'Main Qunatity Flowrate'
threshold = 0

# Iterate over each column and compute Mode, Median, and Mean for non-zero values
for column in df_selected_columns.columns:

    if column == 'Main Qunatity Flowrate (M³/h)':
        non_zero_values = df_selected_columns[column]  # No filtering for 'Main Qunatity Flowrate'
    else:
        non_zero_values = df_selected_columns[column][df_selected_columns[column] > threshold]

    # If the column has no non-zero values, print a message
    if non_zero_values.empty:
        print(f"Column '{column}' contains only zero values or empty data after filtering.")
        print("-" * 70)
    else:
        # Calculate Mode, Median, and Mean
        mode_value = non_zero_values.mode().iloc[0] if not non_zero_values.mode().empty else None
        median_value = non_zero_values.median()
        mean_value = non_zero_values.mean()

        print(f"Column: {column}")
        print(f"Mode: {mode_value}")
        print(f"Median: {median_value}")
        print(f"Mean: {mean_value}")
        print("-" * 70)
