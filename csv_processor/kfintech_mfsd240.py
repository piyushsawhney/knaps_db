import pandas as pd

from postgres import perform_upsert
from utils import validate_pan, validate_email, validate_mobile


def process_pans(pan_column):
    return pan_column.apply(lambda x: validate_pan(x))


df = pd.read_csv("../csvs/rta/kfintech_240.csv")
print(df.columns)

# Validation of PAN columns
df["PH PAN"] = process_pans(df["PH PAN"])
df["J1 PAN"] = process_pans(df["J1 PAN"])
df["J2 PAN"] = process_pans(df["J2 PAN"])
df["GUARDIAN PAN"] = process_pans(df["GUARDIAN PAN"])

# Validation of email columns
df["EMAIL"] = df["EMAIL"].apply(lambda x: validate_email(x))

# Validate mobile
df["MOBILE"] = df["MOBILE"].apply(lambda x: validate_mobile(x))

# Uppercase all names
df["PH NAME"] = df["PH NAME"].apply(lambda x: x.upper() if isinstance(x, str) else None)
df["J1 NAME"] = df["J1 NAME"].apply(lambda x: x.upper() if isinstance(x, str) else None)
df["J2 NAME"] = df["J2 NAME"].apply(lambda x: x.upper() if isinstance(x, str) else None)
df["GUARDIAN NAME"] = df["GUARDIAN NAME"].apply(lambda x: x.upper() if isinstance(x, str) else None)

list_of_columns_name = ["fund_code", "folio_number", "holder1_name", "holder2_name", "holder3_name", "guardian_name",
                        "holder1_pan", "holder2_pan", "holder3_pan", "guardian_pan"]

updated_df = df[
    ['FUND', 'ACNO', 'PH NAME', 'J1 NAME', 'J2 NAME', 'GUARDIAN NAME', 'PH PAN', 'J1 PAN', 'J2 PAN',
     'GUARDIAN PAN']].rename(columns={
    'FUND': 'fund_code',
    'ACNO': 'folio_number',
    'PH NAME': 'holder1_name',
    'J1 NAME': 'holder2_name',
    'J2 NAME': 'holder3_name',
    'GUARDIAN NAME': 'guardian_name',
    'PH PAN': 'holder1_pan',
    'J1 PAN': 'holder2_pan',
    'J2 PAN': 'holder3_pan',
    'GUARDIAN PAN': 'guardian_pan'
})
table_name = 'client_details.folio_details'
values = list(updated_df.itertuples(index=False, name=None))
print(updated_df.columns)

perform_upsert(table_name, updated_df.columns, values, 'folio_number')
