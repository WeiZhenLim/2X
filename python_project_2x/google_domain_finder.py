from googlesearch import search
import pandas as pd
import tldextract
import os

# print notes for user when import
def _print_info():
    print("""
    Things to Take Notes when Using Google Domain Finder: \n
    1. This function only supports CSV file. Please convert your excel file to CSV UTF-8. \n
    2. The list of companies shall be saved with a column title of "Company Name". \n
    3. For better performance, clean your dataset and identify the list of companies with no website. Instead of scraping for all the companies in the dataset. \n
    4. By default, this code excludes "stackoverflow", "youtube", "facebook", "twitter", "linkedin", and "wikipedia". To suppress any additional domain, upload the list of domain in CSV.
    """)

def scrape_comp_domain(filepath, domain_to_exclude_file=""):
    """
    This function takes in the list of company names in CSV format and scrape from Google. The optional argument is to exclude the domain that you wish to exclude.
    """

    df = pd.read_csv(filepath)

    # check for input list formatting
    try:
        df = df[['Company Name']].drop_duplicates()
        lst_comp = df['Company Name'].tolist()
        total_comp = len(lst_comp)
    except KeyError:
        raise Exception("Invalid column name for the list of companies. Column name shall be 'Company Name'.")

    # get URL
    domains = []
    is_good_connection = True
    start = 0

    while is_good_connection:
        try:
            for i in range(start, len(lst_comp)):
                for url in search(lst_comp[i], stop=1):
                    domains.append(url) # append the first google search instances            
                    print(f"Exporting Company no {i+1}: {lst_comp[i]}")
        except Exception: # loop back for any error
            print(f"Fail to export company no {i+1}: {lst_comp[i]}. Try Again.")
            start = i # reset counter to the last error
        else:
            break #End loop if no error occur

    print(f'Scraping complete, {i+1} out of {total_comp} domain being extracted. {"OK" if i+1 == total_comp else "NOT OK"}')

    # save search results in df
    df['URL'] = domains

    # extract company domain
    df['Subdomain'] = df['URL'].apply(lambda x: tldextract.extract(x).subdomain)
    df['Domain'] = df['URL'].apply(lambda x: tldextract.extract(x).domain)
    df['Suffix'] = df['URL'].apply(lambda x: tldextract.extract(x).suffix)
    df['Company Domain'] = df['Domain'] + "." + df['Suffix']

    # clean domain input
    if domain_to_exclude_file == "":
        add_domain_lst = []
    else:
        add_domain_lst = pd.read_csv(domain_to_exclude_file)
        add_domain_lst = add_domain_lst.iloc[:, 0].tolist()
        add_domain_lst = [url.split('.')[0] for url in add_domain_lst]

    # exclude list of domain that need to be exclude
    lst_domain_exclude = ['stackoverflow', 'youtube', 'facebook', 'twitter', 'linkedin', 'wikipedia'] + add_domain_lst
    df['Irrelevant Domain/Domain to Exclude'] = df['Domain'].apply(lambda x: "Yes" if x in lst_domain_exclude else "No")

    # save results as new file
    output_path = os.path.dirname(filepath)
    filename = os.path.basename(filepath).split(".")[0]

    df.to_csv(os.path.join(output_path, filename + " - Output.csv"), index=False)

# NOTE: Test Run
if __name__ == '__main__':
    file_input = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\03 google domain finder\Google Domain Test Input.csv"
    domain_exclude = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\03 google domain finder\Company Domain to Exclude.csv"

    scrape_comp_domain(file_input, domain_exclude)