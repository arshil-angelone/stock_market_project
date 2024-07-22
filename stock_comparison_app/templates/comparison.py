import pandas as pd

# Define the file paths
uat_file_path = '~/Desktop/nse_equity_uat.csv'
prod_file_path = '~/Desktop/nse_equity_prod.csv'

# Load the data
uat_df = pd.read_csv(uat_file_path)
prod_df = pd.read_csv(prod_file_path)

# Define the columns to compare
columns_to_compare = ['openprice', 'highprice', 'lowprice', 'closeprice', 'volume']

# Initialize lists to hold matching and non-matching tokens
matching_tokens = []
non_matching_tokens = []

# Iterate over each token and compare the columns
for token in uat_df['token'].unique():
    uat_row = uat_df[uat_df['token'] == token].iloc[0]
    prod_row = prod_df[prod_df['token'] == token].iloc[0]
    
    mismatch_columns = []
    for col in columns_to_compare:
        if uat_row[col] != prod_row[col]:
            mismatch_columns.append(col)
    
    if mismatch_columns:
        non_matching_tokens.append((token, mismatch_columns))
    else:
        matching_tokens.append(token)

# Print the results
print("Matching Tokens:")
print(matching_tokens)
print("\nNon-Matching Tokens and Columns with Mismatches:")
for token, mismatches in non_matching_tokens:
    print(f"Token: {token}, Mismatched Columns: {mismatches}")
