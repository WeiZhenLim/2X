import pandas as pd
import os

from logicsource_icp_check.icp_check import zi_icp_check

def read_check(filepath_read, is_company, output_path, output_filename):
    """
    This function checks the ZI purchased companies/contacts based on LogicSource's ICP and export the output in 
    HubSpot format for the ease of upload. \n
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
    df_check = zi_icp_check(df, filepath_read, is_company)
 
    # save
    if len(output_filename.split(".")) == 1: #Filename only, not file extension
        df_check.to_csv(os.path.join(output_path, output_filename + ".csv"), index=False)
    elif output_filename.split(".")[1] == "csv": #Filename contains ".csv" extension
        df_check.to_csv(os.path.join(output_path, output_filename), index=False)
    else: #If non CSV File Extension appear
        raise Exception("Incorrect File Extension. Only Accept CSV File Type Or Raw Filename Without Extension.")