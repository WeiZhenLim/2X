import pandas as pd
import re
import os
from IPython.display import display 

# This function label the email as invalid based on certain criteria from the IPQS results
def _invalid_criteria(row):
    """
    This function label the email as invalid if any one of the following criteria is met:
    1. Recent Abuse = TRUE
    2. Valid = FALSE
    3. Disposable = TRUE
    4. Honeypot = TRUE
    5. Spam Trap Score = High
    """

    recent_abuse = row['Recent_Abuse'] == True
    not_valid = row['Valid'] == False
    disposable = row['Disposable'] == True
    honeypot = row['Honeypot'] == True
    spam_score = row['Spam_Trap_Score'] == "high"

    # return true if any one of the criteria is true
    is_invalid = any([recent_abuse, not_valid, disposable, honeypot, spam_score])

    return "Invalid" if is_invalid else "Valid"

# This function compiles all the invalid reasons and display in a new column
def _invalid_reason(row):
    """
    This function compiles all the invalid reasons in a list and display in a new column.
    """
    reasons = []

    recent_abuse = row['Recent_Abuse'] == True
    not_valid = row['Valid'] == False
    disposable = row['Disposable'] == True
    honeypot = row['Honeypot'] == True
    spam_score = row['Spam_Trap_Score'] == "high"

    invalid_bool = [recent_abuse, not_valid, disposable, honeypot, spam_score]
    invalid_label = ['Recent Abuse', 'Invalid Email', 'Disposable Email', 'Honeypot Found', 'High Spam Score']

    if row['Email_Validation'] == "Valid":
        return ""

    for i, v in zip(invalid_bool, invalid_label):
        if i:
            reasons.append(v)
    
    return "; ".join(reasons)

# This function takes in IPQS results (CSV). Then checks for email validity and compiles the invalid reasons and no of invalid reasons.
def ipqs_check_reason(filepath):
    """
    This function takes in IPQS results (CSV). Then checks for email validity and compiles the invalid reasons and no of invalid reasons.
    """

    df = pd.read_csv(filepath)

    # rename column to remove blank and certain pattern
    df_col = df.columns
    replace_pattern = r'[ ()\/:]+'

    df_col_rename = {col: re.sub(replace_pattern, "_", col) for col in df_col}
    df.rename(columns= df_col_rename, inplace=True)

    print("Input data:")
    display(df.head())

    # label and check email validity
    df['Email_Validation'] = df.apply(_invalid_criteria, axis=1)

    # compile invalid reasons 
    df['Invalid_Reasons'] = df.apply(_invalid_reason, axis=1)
    df['No_of_Invalid_Reasons'] = df['Invalid_Reasons'].apply(lambda x: len(x.split(";")) if x != "" else 0)

    # extract file directory and filename
    file_dir = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    
    full_filename = file_dir + '/IPQS Checked-' + filename

    print("Output:")
    display(df.head())
    print(f"Output ({filename}) is save at {file_dir}")

    df.to_csv(full_filename, index=False)

# NOTE: Test
if __name__ == '__main__':
    filepath = r"C:\Users\WeiZhenLim\OneDrive - 2X LLC\Work\Python\python_project_2x\01 Test\04 ipqs reason\LS-Backlog Batch 1-Finance-IPQS Result.csv"

    ipqs_check_reason(filepath)