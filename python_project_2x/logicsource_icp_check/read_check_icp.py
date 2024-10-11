import pandas as pd
import os

from logicsource_icp_check.icp_check import zi_icp_check

def read_check(filepath_read, is_company, output_path, output_filename, revenue_range):
    """
    This function checks the ZI purchased companies/contacts based on LogicSource's ICP and export two output,
    depending on the condition: 
    1. For ZI purchased companies, only export result HubSpot format.
    2. For ZI purchased contacts, export result in HubSpot format and list of email for IPQS.\n
    ---
    Parameters: \n
    `filepath_read` = Path for ZI Purchased Companies/Contacts CSV File \n
    `is_company` = True for ZI Company Data; False for ZI Contact Data (Default = True) \n
    `output_path` = Destination Path to Save the Output \n
    `output_filename` = Filename for the Output. 
    """

    # read file
    df = pd.read_csv(filepath_read)

    # check
    if is_company:
        df_check = zi_icp_check(df, filepath_read, is_company, revenue_range)
    else:
        df_temp = zi_icp_check(df, filepath_read, is_company, revenue_range) # if not company, return tuple of two
        df_check = df_temp[0] # index for contact check
        df_ipqs = df_temp[1] # index for list of email

    # save df_check
    if len(output_filename.split(".")) == 1: #Filename only, not file extension
        df_check.to_csv(os.path.join(output_path, output_filename + ".csv"), index=False)
    elif output_filename.split(".")[1] == "csv": #Filename contains ".csv" extension
        df_check.to_csv(os.path.join(output_path, output_filename), index=False)
    else: #If non CSV File Extension appear
        raise Exception("Incorrect File Extension. Only Accept CSV File Type Or Raw Filename Without Extension.")
    
    # save df_ipqs
    ipqs_prefix = "LS-IPQS-"
    if not is_company:
        if len(output_filename.split(".")) == 1: #Filename only, not file extension
            df_ipqs.to_csv(os.path.join(output_path, ipqs_prefix + output_filename + ".csv"), index=False)
        elif output_filename.split(".")[1] == "csv": #Filename contains ".csv" extension
            df_ipqs.to_csv(os.path.join(output_path, ipqs_prefix + output_filename), index=False)
        else: #If non CSV File Extension appear
            raise Exception("Incorrect File Extension. Only Accept CSV File Type Or Raw Filename Without Extension.")

# NOTE: Functions for Test Run
def _test_company():
    filepath_read = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\05 LS zi ICP check\ZI Company Purchase.csv"
    is_company = True
    output_path = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\05 LS zi ICP check"
    output_filename = "Testing-Company-Oct11"

    read_check(filepath_read, is_company, output_path, output_filename, 1000000)

def _contact_company():
    filepath_read = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\05 LS zi ICP check\ZI Contact Purchase.csv"
    is_company = False
    output_path = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\05 LS zi ICP check"
    output_filename = "Testing-Contact-Oct11.csv"

    read_check(filepath_read, is_company, output_path, output_filename, 1000000)

# def _may15_update():
#     filepath_read = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\05 LS zi ICP check\Company for May 15 Manufacturing and Retail + CPG Issues.csv"
#     is_company = True
#     output_path = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\05 LS zi ICP check"
#     output_filename = "Testing-Company for May 15 Manufacturing and Retail + CPG Issues"

#     read_check(filepath_read, is_company, output_path, output_filename)

# NOTE: Test Run
if __name__ == "__main__":
    
    # ------------------------------------------------------------------------------------------------
    # Version 1 Testing
    # _test_company()
    # print("Done testing for company info.")
    # _contact_company()
    # print("Done testing for contact info.")
    # ------------------------------------------------------------------------------------------------

    # _may15_update()
    # print("Done testing for May 15 update for 'Manufacturing' and 'Retail + CPG'.")

    _test_company()
    _contact_company()
    print("Done testing for Oct 11 Updates")
