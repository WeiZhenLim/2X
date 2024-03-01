import pandas as pd
import os
from datetime import datetime

date_code_execute = datetime.today().strftime("%Y%m%d")

# Function to compile all csv in a folder (Also allowing users to skip rows based on column name) and return the compiled CSV file
def compile_csv(folder_path, dest_path, output_filename, first_col=""):

    """
    This function takes a folder path and compile all csv files in the folder into a single CSV file. The user can also specify
    the first column name to skips rows. Make sure that the first column is always on the leftmost column in the CSV file. \n
    ----------------------------------------------------------------
    Parameters: \n
    `folder_path` = Path for the folder containing the CSV files \n
    `dest_path` = Destination path for the compiled CSV file \n
    `output_filename` = Output filename for the compiled CSV file \n
    `first_col` = The name of the first column to skip rows. Leave it empty if there are no rows to skip \n
    """

    # Get all csvs files in the given directory
    all_files = os.listdir(folder_path)

    # Create a list of all csv files
    csv_files = [f for f in all_files if f.endswith(".csv")]

    # Create a list to hold all data frames
    df_list = []

    # Loop through each csv file
    for csv in csv_files:
        
        csv_file = os.path.join(folder_path, csv)
        print(f"{csv}") # Tell users which csv file is being processed

        col_name = ""

        try: # Try reading CSV using default UTF8 encoding
            i = 0
            
            # Identify which row to skip
            while col_name != first_col: # If first_col is empty, no row is skipped
                df = pd.read_csv(csv_file, skiprows=i, nrows=1)
                col_name = df.columns[0]
                i += 1

            # Read the CSV file
            df = pd.read_csv(csv_file, skiprows=i)

            # Tell users how many rows being skipped and how many rows and columns being read
            print(f"Skipping {i} rows")
            print(f"{csv} contains {df.shape[0]} rows and {df.shape[1]} columns.")

            # Append the csv file in df_list
            df_list.append(df)
        
        except UnicodeDecodeError: # If UTF-8 cannot be decoded
            try: # Try reading CSV using UTF-16 encoding with tab separators
                i = 0
                
                # Identify which row to skip
                while col_name!= first_col: # If first_col is empty, no row is skipped
                    df = pd.read_csv(csv_file, sep="\t", encoding="utf-16", skiprows=i, nrows=1)
                    col_name = df.columns[0]
                    i += 1

                # Read the CSV file
                df = pd.read_csv(csv_file, sep="\t", encoding="utf-16", skiprows=i)

                # Tell users how many rows being skipped and how many rows and columns being read
                print(f"Skipping {i} rows")
                print(f"{csv} contains {df.shape[0]} rows and {df.shape[1]} columns.")

                # Append the csv file in df_list
                df_list.append(df)

            except Exception as e:
                print(f"Could not write read file {csv} because of error: {e}")

        except Exception as e:
            print(f"Could not read file {csv} because of error: {e}")
    
    # Concatenate all data frames in df_list
    df_compile = pd.concat(df_list, ignore_index=True)

    # Get output_filename without extension (User can either give the filename with or without extension) 
    output_filename = output_filename.split(".")[0] + ".csv"

    # Export as CSV
    df_compile.to_csv(os.path.join(dest_path, date_code_execute + "-" + output_filename), index=False)
    print(f"Successfully compiled. Total no of rows = {df_compile.shape[0]}")

    return df_compile

# Test Code
if __name__ == "__main__":
    folder_path = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\01 compile_csv Test\Compile Without Skipping"
    dest_path = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\01 compile_csv Test"
    output_filename = r"Testing - No Skip"
    first_col = "Start Date (in UTC)"
    compile_csv(folder_path, dest_path, output_filename)