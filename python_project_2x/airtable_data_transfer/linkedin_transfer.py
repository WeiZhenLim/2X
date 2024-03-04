import pandas as pd
from IPython.display import display
import urllib.parse as urlparse
from urllib.parse import parse_qs
import os
import sys
import re

# Objective Keywords Mapping Dictionary {"Stage": "Objective Keywords"}
campaign_obj = {"Awareness" : ["Brand awareness"],
                "Consideration" : ["Website visits", "Engagement", "Video views", "Messaging"],
                "Conversions" : ["Lead generation", "Talent leads", "Website conversions", "Job applicants"]}


# Function to extract url's components (website and utm parameters)
def _url_to_website_utm(url, component):
    """
    This function is used to extract url's components (website and utm parameters). \n
    ----------------------------------------------------------------
    Parameters: \n
    `url` = url link \n
    `component` = url component to return. Accepted values are "website", "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", and "utm_id". \n
    """
    
    # Define the required components as a dictionary to store the result
    url_comp = {"website":"", 
                "utm_source": "", 
                "utm_medium": "", 
                "utm_campaign": "", 
                "utm_content":"", 
                "utm_term":"", 
                "utm_id":""}
    
    # If url is empty, return empty string for any component
    if url == "":
        return url_comp[component]

    # If url is not string, return empty string for any component
    if not isinstance(url, str):
        return url_comp[component]
    
    # Parse url
    parsed_url = urlparse.urlparse(url)

    # Extract website and store in the utm_comp dictionary
    website = parsed_url[0] + "://" + parsed_url[1] + parsed_url[2]
    url_comp["website"] = website

    # Extract utm parameters
    # If utm is empty, return empty string for utm parameters
    if parsed_url.query == "":
        return url_comp[component]
    
    # If utm is not empty, get the utm parameters
    utm_list = parsed_url.query.split("&")

    # Loop through each utm parameter in utm_split and assign it to the utm_comp dictionary
    for utm in utm_list:
        
        utm_comp = utm.split("=")
        
        utm_type = utm_comp[0]

        utm_para = utm_comp[1]

        url_comp[utm_type] = utm_para

    return url_comp[component]

# Function for Objective Keywords Mapping in LinkedIn
def _campaign_keywords_mapping(row):
    """
    This function maps the objective keywords in LinkedIn to the corresponding stage in a pandas table.
    """
    cpg_obj = row['Campaign Objective']

    for stage, obj_lst in campaign_obj.items():
        if cpg_obj in obj_lst:
            return stage
    
    raise ValueError(f"Invalid Objective Found {cpg_obj}. Please Check the Exported CSV File")

# Function to check the number of campaign groups, campaign, and ads
def check_export_count(data, col_list=['Campaign Group ID', 'Campaign ID', 'Ad ID']):
    """
    This function is used to check the number of campaign groups, campaign, and ads being extracted and compiled. \n
    ----------------------------------------------------------------
    Parameters: \n
    `data` = pandas data frame \n
    `col_list` = list of columns to check. Default list of columns are "Campaign Group ID", "Campaign ID", and "Ad ID". \n
    """
    # Aggregate the campaign groups, campaign, and ads
    result = data[col_list].apply(lambda col: col.nunique())

    display(result)

# Function to calculate the launch date and completed date for each ads
def get_launch_completed_date(data):
    """
    This function is used to calculate the launch date and completed date for each ads, then return the aggregated results.
    With the assumption of launch date = first day with clicks; completed date = last day with clicks.
    ----------------------------------------------------------------
    Parameters: \n
    `data` = pandas data frame \n
    """
    # Initial no of rows for data
    no_of_rows = data.shape[0]

    # Filter data to exclude clicks = 0
    data = data[data['Clicks'] > 0].copy()

    # Create a copy of the subset of the data frame
    data_date = data[['Ad ID', 'Start Date (in UTC)']].copy()

    # Tell the users how many rows with clicks = 0
    print(f"There are {no_of_rows - data_date.shape[0]} instances with zero clicks. Original data: {no_of_rows} instances; Filtered data: {data_date.shape[0]} instances.")

    # Convert the Start Date (in UTC) to datetime format
    data_date['Date'] = pd.to_datetime(data_date['Start Date (in UTC)'])

    # Get the maximum and minimum date for each ads
    launch_comp_date = pd.pivot_table(data_date, values='Date', index=['Ad ID'], aggfunc=['min', 'max']).reset_index()

    # Rename the column names
    launch_comp_date.rename(columns={'min': 'Launch date', 'max': 'Completed date'}, inplace=True)

    # Drop MultiIndex
    launch_comp_date = launch_comp_date.droplevel(1, axis=1)

    return launch_comp_date

# Function to clean column names for the dataset
def _clean_column_names(data):
    """
    This function is used to clean column names for the dataset and returns the cleaned data. \n
    ----------------------------------------------------------------
    Parameters:\n
    `data` = pandas data frame \n
    """
    # Create a copy of the input data frame
    data = data.copy()

    # Get a list of column names
    col_names = data.columns.tolist()

    # Pattern to retain
    pattern = r"[^a-zA-Z0-9() ]+"

    # Clean the column names by removing all symbols except parenthesis
    col_names = [re.sub(pattern, "", col) for col in col_names]

    # Replace the column names
    data.columns = col_names

    return data

# Function to preprocess ads performance report data
def preprocess_ads_performance_report(data):
    """
    This function is used to preprocess ads performance report data and return the preprocessed data. 
    The preprocessing steps are:
    1. Clean column names (To remove symbols).
    2. Extract website and utm parameters from url (And Remove Ori URL Column).
    3. Staging remap based on campaign objectives.
    4. Subset relevant columns.
    5. Drop duplicates.
    ----------------------------------------------------------------
    Parameters: \n
    `data` = pandas data frame \n
    """
    # Create a copy of the input data frame
    data = data.copy()

    # Clean column names (To remove symbols)
    data = _clean_column_names(data)

    # Extract website and utm parameters from url (And Remove Ori URL Column)
    data['Website URL'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "website"))
    data['utm_source'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "utm_source"))
    data['utm_medium'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "utm_medium"))
    data['utm_campaign'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "utm_campaign"))
    data['utm_content'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "utm_content"))
    data['utm_term'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "utm_term"))
    data['utm_id'] = data['Click URL'].apply(lambda x: _url_to_website_utm(x, "utm_id"))

    # Staging remap based on campaign objectives
    data['Stage'] = data.apply(_campaign_keywords_mapping, axis=1)

    # Subset relevant columns
    req_col = ["Account Name", 
               "Campaign Group ID", "Campaign Group Name", "Campaign Group Status", 
               "Campaign ID", "Campaign Name", "Campaign Objective", "Campaign Type", "Campaign Status", 
               "Cost Type",
               "Creative Name", "Ad ID",
               "Sponsored Update Type", "DSC Name", "Video Length (in Seconds)"]
    data = data[req_col]

    # Drop duplicates
    data.drop_duplicates(inplace=True)

    return data

# Preprocess Ads LinkedIn Bulk Export
def preprocess_ads_bulk_report(data):
    """
    This function is used to preprocess ads performance report data and return the preprocessed data. 
    The preprocessing steps are:
    1. Clean column names (To remove symbols).
    2. Subset relevant columns.
    3. Rename the "Creative ID" column to "Ad ID".
    ----------------------------------------------------------------
    Parameters: \n
    `data` = pandas data frame \n
    """
    # Create a copy of the input data frame
    data = data.copy()

    # Clean column names (To remove symbols)
    data = _clean_column_names(data)

    # Subset relevant columns
    req_col = ["Account ID",
               "Creative ID", "Creative Status", "Ad format", "Ad name", 
               "Introductory", "Headline", "Description", "Call to action", "Destination URL"]
    data = data[req_col]

    # Rename the "Creative ID" column to "Ad ID"
    data.rename(columns={"Creative ID": "Ad ID"}, inplace=True)

    return data

# "Ad name" and "Creative Name" columns validation
def _validate_ad_name(row):
    """
    This function is used to validate the "Ad name" column from `df_report` and "Creative Name" column from `df_bulk` based
    on the following criteria:
    1. If "Ad name" == "Creative Name", return "Ad name".
    2. If "Ad name" == "", return "Creative Name".
    3. If "Creative Name" == "", return "Ad name".
    4. If both "Ad name" and "Creative Name" == "", return "".
    """
    if row['Ad name'] == row['Creative Name']:
        return row['Ad name']
    elif row['Ad name'] == "":
        return row['Creative Name']
    elif row['Creative Name'] == "":
        return row['Ad name']
    else:
        return ""
    

# Merge and preprocess the merge data
def merge_and_preprocess(df_report, df_bulk):
    """
    This function is used to merge and preprocess the merge data.
    The preprocessing steps are:
    1. Merge the two data frames.
    2. Validate the "Ad name" column in `df_report` and "Creative Name" column in df_bulk.
    3. Drop "Ad name" and "Creative Name" columns.
    ----------------------------------------------------------------
    Parameters: \n
    `df_report` = processed ads performance report pandas data frame \n
    `df_bulk` = processed ads bulk report pandas data frame \n
    """
    # Create a copy of the input data frame
    df_report = df_report.copy()
    df_bulk = df_bulk.copy()

    # Merge two data frames
    df_final = pd.merge(df_report, df_bulk, how='left', on="Ad ID") # Using left merge because bulk export can only export single image and text ads only

    # Validate the "Ad name" and "Creative Name" columns
    df_final['Ad Name'] = df_final.apply(_validate_ad_name, axis=1)

    # Drop "Ad name" and "Creative Name" columns
    df_final.drop(columns=["Ad name", "Creative Name"], inplace=True)

    return df_final

# Test Code
if __name__ == "__main__":
    """
    Workflow:
    1. Read Ad Performance Report (Compile if necessary)
    2. Read LinkedIn Ads Bulk Report (Compile if necessary)
    3. Get launch and completed date from Ad Performance Report
    4. Preprocess Ad Performance Report
    5. Preprocess LinkedIn Ads Bulk Report
    """
    from csv_hacks import compile_csv

    folder_path_report = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\airtable_data_transfer\Ad Performance Export Example"
    folder_path_bulk = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\airtable_data_transfer\Ads Bulk Export Example"
    dest_folder = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\airtable_data_transfer"

    print("------------------------------------------------------------------------------------")

    # Read Ad Performance Report (Compile if necessary)
    df_report = compile_csv(folder_path_report, dest_folder, "ad_performance_report", "Start Date (in UTC)")

    # Read LinkedIn Bulk Report (Compile if necessary)
    df_bulk = compile_csv(folder_path_bulk, dest_folder, "ads_bulk_report", "*Account ID")

    # Get launch and completed date from df_report
    df_launch_completed_date = get_launch_completed_date(df_report)

    # Preprocess Ad Performance Report
    df_report = preprocess_ads_performance_report(df_report)

    # Preprocess LinkedIn Ads Bulk Report 
    df_bulk = preprocess_ads_bulk_report(df_bulk)

    print("------------------------------------------------------------------------------------")

