import pandas as pd
import os

def main():
    initial_files_dir = 'initial_files'
    final_file_path = 'merged/final_file.csv'
    output_dir = 'comparison_output'
    
    # Read the final file
    df_final = pd.read_csv(final_file_path)
    
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Iterate through the initial files
    for filename in os.listdir(initial_files_dir):
        if filename.endswith('.csv'):
            initial_file_path = os.path.join(initial_files_dir, filename)
            
            # Read the initial file
            df_initial = pd.read_csv(initial_file_path)
            
            # Compare the dataframes
            merged_df = pd.merge(df_initial, df_final, how='outer', indicator=True)
            diff_df = merged_df.loc[merged_df['_merge'] != 'both']
            
            if not diff_df.empty:
                output_file_path = os.path.join(output_dir, f"differences_{filename}.csv")
                
                # Save differences as CSV file
                diff_df.to_csv(output_file_path, index=False)
                    
                print(f"Differences found between {filename} and final_file.csv. Saved to {output_file_path}.")
            else:
                print(f"No differences found between {filename} and final_file.csv.")
        else:
            print(f"Skipped file {filename} as it is not a CSV file.")
    
if __name__ == "__main__":
    main()
