import pandas as pd
import tldextract as domain_extract
from urllib.error import HTTPError
from datetime import datetime
from IPython.display import display

# ----------------------------------------------------------------------------------------------------------------------------------------------------

# ICP - Segment
# Get list for company to exclude & industry to exclude

# Read CSV From GitHub
comp_csv = r'https://raw.githubusercontent.com/WeiZhenLim/2X/main/python_project_2x/logicsource_icp_check/Company%20to%20Exclude.csv'
industry_csv = r'https://raw.githubusercontent.com/WeiZhenLim/2X/main/python_project_2x/logicsource_icp_check/Industry%20ICP%20Keywords.csv'

try:
    comp_to_exclude = list(pd.read_csv(comp_csv)['Company Domain'].unique())
    
    industry_segmentation_df = pd.read_csv(industry_csv)[['Industry Keyword', 'Segments']]
    industry_segmentation = industry_segmentation_df.groupby('Segments')['Industry Keyword'].apply(list).to_dict()
except HTTPError as e:
    raise Exception("Check and Update Raw GitHub URL.")

# Healthcare Industry
healthcare_ind_list = ['Ambulance Services', 'Blood & Organ Banks', 'Elderly Care Services', 
                        'Medical Laboratories & Imaging Center', 'Medical Laboratories & Imaging Centers', 
                        'Dental Offices', 'Medical & Surgical Hospitals', 'Medical Specialists', 'Physicians Clinics', 
                        'Hospitals & Physicians Clinics'] 

# Create an Output for User to Download the latest version of the segmentation
def _print_info():

    print("The following are the Industry ICP used for the ICP check (Original Industry ICP dated back to Nov 2023)")
    display(industry_segmentation_df.groupby('Segments')['Industry Keyword'].apply(list).reset_index())

# ----------------------------------------------------------------------------------------------------------------------------------------------------

# This section is completed on Feb 7, 2024 by Lim Wei Zhen
# New update on Feb 15, 2024 by Lim Wei Zhen -> To add a new column to mark our ownership and one new column for IPQS Check
# New update on Oct 11, 2024 by Lim Wei Zhen -> Change the way industry is being segmented

# ZI Raw Data Preprocessing
def _zi_preprocessing(data, filename="", is_company=True):
    """
    This function is used to preprocess ZI Data for the later stage \n
    data = ZI Data \n
    filename = Filename of the ZI Company Data (For Backtracking Purpose)
    is_company = True for ZI Company Data; False for ZI Contact Data (Default = True)
    """

    # Subset required columns
    try:
        if is_company:
            data = data[['Company Name', 'Website', 'Company HQ Phone', 'Revenue (in 000s USD)', 'Revenue Range (in USD)',
                        'Primary Industry', 'Primary Sub-Industry', 'All Industries', 'All Sub-Industries',
                        'LinkedIn Company Profile URL', 'Facebook Company Profile URL', 'Twitter Company Profile URL', 
                        'Company Street Address', 'Company City', 'Company State', 'Company Zip Code', 'Company Country']].copy()
        else:
            data = data[['First Name', 'Last Name', 'Job Title', 'Job Function', 'Management Level', 
                        'Email Address', 'Email Domain', 'Direct Phone Number', 'LinkedIn Contact Profile URL', 
                        'Person Street', 'Person City', 'Person State', 'Person Zip Code', 'Country', 
                        'Company Name', 'Website', 'Company HQ Phone', 'Revenue (in 000s USD)', 'Revenue Range (in USD)', 
                        'Primary Industry', 'Primary Sub-Industry', 'All Industries', 'All Sub-Industries', 
                        'LinkedIn Company Profile URL', 'Facebook Company Profile URL', 'Twitter Company Profile URL',
                        'Company Street Address', 'Company City', 'Company State', 'Company Zip Code', 'Company Country']].copy()
    except KeyError:
        print(f"Check for the following:")
        print(f"1. Input data (file) matches with the 'is_company` parameter. If `is_company` = True (By default is True), input data = ZI Company Data; If `is_company` = False, input data = ZI Contact Data")
        print(f"2. There is a change in the column names from the ZI Company/Contact Data.")

    # Column to track source
    data['Source-Checked On (YYYYMMDD)'] = filename + "-" + str(datetime.today().strftime('%Y%m%d'))

    # Column to track ownership
    data['Enrich/Expand By 2X (YYYYMMDD)'] = "Enrich/Expand by 2X on " + str(datetime.today().strftime('%Y%m%d'))

    # Get domain
    data['Company Domain'] = data['Website'].apply(lambda x: domain_extract.extract(x).domain + "." + domain_extract.extract(x).suffix)

    # Add column for industry standardized, (2X)Lead Segment Nov 2023 & Lead Segment HS
    data['Industry (Standardized)'] = ""
    data['(2X)Lead Segment Nov 2023'] = ""
    data['Lead Segment HS'] = ""

    # Add column for validation/check
    data['Valid/Invalid'] = ""
    data['Remark'] = ""
    data['Industry_ICP_Check_List'] = ""
    data['IPQS Check'] = ""

    # Compile all four industry columns into one list for later stage
    industry_col = ['Primary Industry', 'Primary Sub-Industry', 'All Industries', 'All Sub-Industries']

    data['ZI Industry List'] = [[] for _ in range(len(data))]

    for col in industry_col:
        data['ZI Industry List'] = data['ZI Industry List'] + data[col].astype(str).str.split(";")

    # Create 'Job Role (Standardized)', "Membership Note" and "Source (Self-Define)" column (Only for Contact Data)
    if not is_company:
        data['Job Role (Standardized)'] = ""
        data['Membership Note'] = ""
        data['Source (Self-Define)'] = ""

    return data

# ZI Industry Check
def _industry_check(row):
    """
    This function will segment the industry based on the segmentation in the "Industry ICP Keywords.csv" file.
    """

    # create an empty list to store the results 
    ind_result = set()

    for industry in row: 

        for segment, ind_icp in industry_segmentation.items():

            if industry in ind_icp:
                if "Banking" in industry:
                    ind_result.add("Banking")
                else:
                    ind_result.add(segment)

    return ind_result

# ZI ICP Check
def zi_icp_check(data, filename="", is_company=True, revenue_range = 1000000):
    """
    This function is use to check the ICP for the Company/Contact Data purchased from ZI \n
    data = ZI Company/Contact Data \n
    filename = Filename of the ZI Company/Contact Data (For Backtracking Purpose) \n
    is_company = True for ZI Company Data; False for ZI Contact Data (Default = True)
    """

    # Preprocessing data for ICP Check
    data = _zi_preprocessing(data, filename, is_company)

    # ------------------------------------------------------------------------------------------

    # Person Location ICP Check
    country_icp = ["United States", "Canada"]

    if not is_company:
        def person_location_check(row):
            if row['Country'] not in country_icp:
                return "Person Not In US/CA"
            else:
                return ""
        
        data['Remark'] = data.apply(person_location_check, axis=1)

    # ------------------------------------------------------------------------------------------

    # Revenue ICP Check
    def check_revenue(row):    
        if row['Remark'] != "":
            return row['Remark']
        elif row['Revenue (in 000s USD)'] < revenue_range:
            return "Invalid Revenue Range"
        else:
            return ""
    
    data['Remark'] = data.apply(check_revenue, axis=1)

    # ------------------------------------------------------------------------------------------

    # Check for location
    def country_check(row):
        # only label those that have revenue > 100,000
        if row['Remark'] != "":
            return row['Remark']
        elif row['Company Country'] not in country_icp:
            return "Company Not In US/CA"
        else:
            return ""

    data['Remark'] = data.apply(country_check, axis=1)

    # ------------------------------------------------------------------------------------------

    # Check for company to exclude
    
    def exclude_company(row):
        if row['Remark'] != "":
            return row['Remark']
        elif row['Company Domain'] in comp_to_exclude:
            return "Company In Client List"
        else:
            return ""
        
    data['Remark'] = data.apply(exclude_company, axis=1)    

    # ------------------------------------------------------------------------------------------        

    # Check for Industry ICP
    def industry_ICP_check(row):
        if row['Remark'] != "":
            return row['Remark']
        elif "Others" in row['Industry_ICP_Check_List']:
            return "Manual Check for Industry"
        elif ("Banking" in row['Industry_ICP_Check_List']) and (row['Revenue (in 000s USD)'] > 15000000):
            return "Invalid Sub-Industry (Banking with Revenue > $15B)"
        elif "Non ICP" in row['Industry_ICP_Check_List']:
            return "Invalid Industry"
        else:
            return ""
    
    # Call industry_check
    data['Industry_ICP_Check_List'] = data['ZI Industry List'].apply(lambda x: _industry_check(x))

    data['Remark'] = data.apply(industry_ICP_check, axis=1)

    # ------------------------------------------------------------------------------------------

    # Standardized Industry
    def industry_standardized(row):
        if row['Remark'] == "Manual Check for Industry":
            return "Manual Check for Industry"
        elif row['Remark'] != "":
            return "Non ICP"
        
        # temp holder to consider healthcare keywords appearing in services
        ind_temp = []

        for ind in row['Industry_ICP_Check_List']:  
            
            if ind == "Healthcare": # If it's healthcare keyword, then get the keyword from Healthcare
                for healthcare_ind in row['ZI Industry List']:
                    if healthcare_ind in healthcare_ind_list:
                        if healthcare_ind == "Hospitals & Physicians Clinics":
                            return "Physicians Clinics"
                        return healthcare_ind
            else:
                ind_temp.append(ind)
        
        if len(ind_temp) == 0:
            return "Manual Check for Industry"

        return ind_temp[0]
                
    data['Industry (Standardized)'] = data.apply(industry_standardized, axis=1)

    # ------------------------------------------------------------------------------------------

    # Lead Segment HS
    def resegmentation_ICP(row):
        if row['Remark'] == "Manual Check for Industry":
            return ""
        elif row['Remark'] != "":
            return "Non ICP"
        elif row['Industry (Standardized)'] in healthcare_ind_list:
            return "Healthcare"
        else:
            return row['Industry (Standardized)']

    data['Lead Segment HS'] =  data.apply(resegmentation_ICP, axis=1)

    # ------------------------------------------------------------------------------------------

    # NOTE: May 15, 2024 - New "Manufacturing" and "Retail + CPG" segments criteria (This criteria is removed on Oct 11 2024)
    
    # """
    # 1. To check whether "Lead Segment HS" = "Retail + CPG" or "Manufacturing". If yes, then continue.
    # 2. If "Lead Segment HS" = "Retail + CPG" and "retail + CPG to manufacturing" in "Industry_ICP_Check_List", 
    # then "Lead Segment HS" and "Industry (Standardized)" = "Manufacturing - Nicole to Check".
    # 3. If "Lead Segment HS" = "Manufacturing", `may15_retail_condition` in "Industry_ICP_Check_List", and "manufacturing to retail + CPG - exclude" not in "Industry_ICP_Check_List", 
    # then "Lead Segment HS" = "Retail + CPG" and "Industry (Standardized)" = "Retail".
    # """

    # may15_retail_condition = {"retail", "manufacturing", "manufacturing to retail + CPG"}

    # def may15_ind(row):
    #     if (row["Lead Segment HS"] == "Retail + CPG") or (row["Lead Segment HS"] == "Manufacturing"):
    #         if (row["Lead Segment HS"] == "Retail + CPG") and ("retail + CPG to manufacturing" in row["Industry_ICP_Check_List"]):
    #             return "Manufacturing - Nicole to Check"
    #         elif (row["Lead Segment HS"] == "Manufacturing") and (may15_retail_condition.issubset(set(row["Industry_ICP_Check_List"]))) \
    #             and ("manufacturing to retail + CPG - exclude" not in row["Industry_ICP_Check_List"]):
    #             return "Retail - Updated"
    #         else:
    #             return row["Industry (Standardized)"]
    #     else:
    #         return row["Industry (Standardized)"]

    # def may15_lead_segment(row):
    #     if (row["Lead Segment HS"] == "Retail + CPG") or (row["Lead Segment HS"] == "Manufacturing"):
    #         if (row["Lead Segment HS"] == "Retail + CPG") and ("retail + CPG to manufacturing" in row["Industry_ICP_Check_List"]):
    #             return "Manufacturing - Nicole to Check"
    #         elif (row["Lead Segment HS"] == "Manufacturing") and (may15_retail_condition.issubset(set(row["Industry_ICP_Check_List"]))) \
    #             and ("manufacturing to retail + CPG - exclude" not in row["Industry_ICP_Check_List"]):
    #             return "Retail + CPG - Updated"
    #         else:
    #             return row["Lead Segment HS"]
    #     else:
    #         return row["Lead Segment HS"]
        
    # data["Industry (Standardized)"] = data.apply(may15_ind, axis=1)
    # data["Lead Segment HS"] = data.apply(may15_lead_segment, axis=1)

    # ------------------------------------------------------------------------------------------

    # (2X)Lead Segment Nov 2023
    def lead_segment_nov23(row):
        if row['Remark'] == "Manual Check for Industry":
            return ""
        elif row['Remark'] != "":
            return "Non ICP"
        elif row['Lead Segment HS'] == "Healthcare":
            return "Healthcare"
        else:
            return "Services"
        
    data['(2X)Lead Segment Nov 2023'] = data.apply(lead_segment_nov23, axis=1)

    # ------------------------------------------------------------------------------------------

    # Label Valid/Invalid column
    def valid_invalid(row):
        if row == "Manual Check for Industry":
            return ""
        elif row == "":
            return "Valid"
        else:
            return "Invalid"

    data['Valid/Invalid'] = data['Remark'].apply(lambda x: valid_invalid(x))


    # ------------------------------------------------------------------------------------------

    # Label Remark column if Empty
    data['Remark'] = data['Remark'].apply(lambda x: "Valid" if x == "" else x)

    
    # Get relevant columns for Contact ICP Check
    output_col_contact = ['First Name', 'Last Name', 'Job Title', 'Job Role (Standardized)', 'Job Function', 'Management Level', 
                          'Email Address', 'Email Domain', 'Direct Phone Number', 'LinkedIn Contact Profile URL', 
                          'Person Street', 'Person City', 'Person State', 'Person Zip Code', 'Country', 
                          'Company Name', 'Website', 'Company Domain', 'Company HQ Phone', 
                          'Revenue (in 000s USD)', 'Revenue Range (in USD)', 
                          'Primary Industry', 'Primary Sub-Industry', 'All Industries', 'All Sub-Industries', 
                          'Industry (Standardized)', '(2X)Lead Segment Nov 2023', 'Lead Segment HS',
                          'LinkedIn Company Profile URL', 'Facebook Company Profile URL', 'Twitter Company Profile URL', 
                          'Company Street Address', 'Company City', 'Company State', 'Company Zip Code', 'Company Country',
                          'Membership Note', 'Source (Self-Define)', 'Enrich/Expand By 2X (YYYYMMDD)', 'IPQS Check',
                          'Valid/Invalid', 'Remark', 'Source-Checked On (YYYYMMDD)']

    # Get relevant columns for Company ICP Check
    output_col_comp = ['Company Name', 'Website', 'Company Domain', 'Company HQ Phone', 
                       'Revenue (in 000s USD)', 'Revenue Range (in USD)',
                       'Primary Industry', 'Primary Sub-Industry', 'All Industries', 'All Sub-Industries', 
                       'Industry (Standardized)', '(2X)Lead Segment Nov 2023', 'Lead Segment HS', 
                       'LinkedIn Company Profile URL', 'Facebook Company Profile URL', 'Twitter Company Profile URL',
                       'Company Street Address', 'Company City', 'Company State', 'Company Zip Code', 'Company Country', 'Enrich/Expand By 2X (YYYYMMDD)', 'IPQS Check',
                       'Valid/Invalid', 'Remark', 'Source-Checked On (YYYYMMDD)']

    # NOTE: The following dictionary is the reformatted version of the column definition for both company and contact datasets (Reformatted on Mar 11, 2024)
    # Source File = https://2xmarketing-my.sharepoint.com/:x:/g/personal/weizhen_lim_2x_marketing/EfDKbx_4CHNJutawnpmp8EcB1RS3rHSTDgzBcDCyBuGNRg?e=F21UCY
    comp_dict_rename = {"Website": "Website URL", "Company Domain": "Company Domain Name", "Company HQ Phone": "Phone number", 
                        "Revenue (in 000s USD)": "Annual Revenue", "Revenue Range (in USD)": "Annual Revenue Range", 
                        "Primary Industry": "Industry", "Primary Sub-Industry": "Sub-Industry", "All Industries": "All Industry", "All Sub-Industries": "All Sub-Industry", 
                        "(2X)Lead Segment Nov 2023": "Account Segment Nov 2023", "Lead Segment HS": "Account Segment HS", 
                        "LinkedIn Company Profile URL": "LinkedIn Company Page", "Facebook Company Profile URL": "Facebook Company Page", 
                        "Twitter Company Profile URL": "Twitter Handle", "Company Street Address": "Street Address", 
                        "Company City": "City", "Company State": "State/Region", "Company Zip Code": "Postal Code", "Company Country": "Country/Region", "Remark": "2x Notes"}
    
    contact_dict_rename = {"Job Role (Standardized)": "Job Role", "Job Function": "Job function", "Email Address": "Email", "Direct Phone Number": "Phone Number", 
                           "LinkedIn Contact Profile URL": "LinkedIn", "Person Street": "Street Address", "Person City": "City", "Person State": "State/Region", 
                           "Person Zip Code": "Postal Code", "Country": "Country/Region", "Website": "Website URL", "Company Domain": "Company website",
                           "Primary Industry": "Industry", "Membership Note": "Membership Notes", "Source (Self-Define)": "Source Type"}


    if not is_company:
        data_contact = data[output_col_contact]
        # Reformat of contact fields
        data_contact = data_contact.rename(columns=contact_dict_rename)

        # Add Tracker Column
        data_contact["2X Tracker - Company Details"] = ""
        data_contact["2X Tracker - Contact Details"] = ""
        data_contact["2X Tracker - Overall Status"] = ""

        email_contact = data[data['Remark'] != "Person Not In US/CA"][['Email Address']] # get email for IPQS
        email_contact = email_contact.rename(columns={'Email Address': 'email'}) # format for IPQS

        return data_contact, email_contact
    else:
        data_comp = data[output_col_comp]
        # Reformat of company fields
        data_comp = data_comp.rename(columns=comp_dict_rename)

        # Add a new column called "2X Tracker"
        data_comp["2X Tracker"] = ""

        return data_comp

# ----------------------------------------------------------------------------------------------------------------------------------------------------

