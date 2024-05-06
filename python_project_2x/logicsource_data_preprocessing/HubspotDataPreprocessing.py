import pandas as pd
import os

# List of columns to be subset from the HubSpot Company Dataset
comp_col = ["Record ID", "Company name", "Company Domain Name", "Country/Region", 
              "Annual Revenue", "Annual Revenue Range", "Industry (Standardized)", 
              "Account Segment Nov 2023", "Account Segment HS", 
              "Enrich/Expand By 2X (YYYYMMDD)", "2x Notes", "2X Tracker"]

# List of columns to be subset from the HubSpot Contact Dataset
contact_col = ["Record ID", "Email", "Email Domain", "Country/Region", 
               "Job Title", "Job Role", "Job Role (Organic)", "Job Role (Reassigned)", "Seniority", 
               "Lead Segment", "Lead Segment HS", "Company Name", "Company website", "Industry (Standardized)", 
               "Marketing contact status", "Membership Notes"]

# NOTE: Create a function to preprocess the company data
def preprocess_company_data(filename, dest_folder, output_filename):
    """
    This function is used to preprocess the HubSpot company data, sub-setting only the important columns/fields.

    Args:
    filename (str): The filename of the HubSpot company data, in CSV format.
    dest_folder (str): The destination folder for the preprocessed company data.
    output_filename (str): The output filename for the preprocessed company data, in xlsx format.

    Output:
    The preprocessed company data is saved in the destination folder with the specified output filename. In the output
    Excel file, there will be a total of 5 sheets, where the "Notes" sheet shall contain the description of each sheet.
    """

    # Validate the file type from filename
    if not filename.endswith(".csv"):
        raise ValueError("The input file must be a CSV file.")

    # Read the HubSpot company data
    df_comp = pd.read_csv(filename)

    # Subset the columns from the HubSpot company data
    df_comp = df_comp[comp_col]

    # Sheet 1: Notes
    note_dict = {"Notes:": 
                 ['1. The first tab, "Notes" is used to describe the meaning of each sheet in the output file.', 
                  '2. The second tab, "Non ICP & PE Firm" is the HubSpot Company Database filtered by "2X Tracker" = "Non ICP" or "PE Firm". The companies in this tab can be ignored for expansion, unless there is a change in ICP (Dated back Nov 2023)',
                  '3. The third tab, "Further Enrich/Expand" is the HubSpot Company Database filtered by "2X Tracker" = "Further Enrich/Expand". The companies in this tab will be used as the list of companies for the Database Re-Expansion.',
                  '4. The fourth tab, "Not Found In ZI" is the HubSpot Company Database filtered by "2X Tracker" = "Not Found In ZI". The companies in this tab can be temporarily ignored, only used this list of companies if there are no more list to expand.',
                  '5. The fifth tab, "Done" is the HubSpot Company Database filtered by "2X Tracker" = "Done". The companies in this tab are the list of companies that have been completed the Database Re-Expansion.']}
    df1 = pd.DataFrame(note_dict)

    # Sheet 2: Non ICP & PE Firm
    df2 = df_comp[(df_comp["2X Tracker"] == "Non ICP") | (df_comp["2X Tracker"] == "PE Firm")]

    # Sheet 3: Further Enrich/Expand
    df3 = df_comp[df_comp["2X Tracker"] == "Further Enrich/Expand"]

    # Sheet 4: Not Found In ZI
    df4 = df_comp[df_comp["2X Tracker"] == "Not Found In ZI"]

    # Sheet 5: Done
    df5 = df_comp[df_comp["2X Tracker"] == "Done"]

    # Validate the output_filename input
    output_filename = output_filename.split(".")[0] + ".xlsx"

    # Create the output filepath
    output_filepath = os.path.join(dest_folder, output_filename)

    # Output as Excel file
    with pd.ExcelWriter(output_filepath) as writer:
        df1.to_excel(writer, sheet_name='Notes', index=False)
        df2.to_excel(writer, sheet_name='Non ICP AND PE Firm', index=False)
        df3.to_excel(writer, sheet_name='Further Enrich OR Expand', index=False)
        df4.to_excel(writer, sheet_name='Not Found In ZI', index=False)
        df5.to_excel(writer, sheet_name='Done', index=False)

    # Inform the user that the preprocessing is completed.
    print(f"The HubSpot Company Database is preprocessed and saved as {output_filename}.")

# NOTE: Test case for preprocess_company_data, for output_filename with extension
if __name__ == '__main__':
    filename = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\06 LS HubSpot Company Data Preprocessing\hubspot-crm-exports-all-companies-2024-05-06-csv.csv"
    dest_folder = r'C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\06 LS HubSpot Company Data Preprocessing'
    output_filename = "HubSpot Company Database Preprocessing.xlsx"

    preprocess_company_data(filename, dest_folder, output_filename)

# NOTE: Test case for preprocess_company_data, for output_filename with extension
if __name__ == '__main__':
    filename = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\06 LS HubSpot Company Data Preprocessing\hubspot-crm-exports-all-companies-2024-05-06-csv.csv"
    dest_folder = r'C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\06 LS HubSpot Company Data Preprocessing'
    output_filename = "HubSpot Company Database Preprocessing 2"

    preprocess_company_data(filename, dest_folder, output_filename)