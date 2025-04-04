import pandas as pd

from postgres import perform_upsert
from utils import validate_pan, validate_email, validate_mobile


def process_pans(pan_column):
    return pan_column.apply(lambda x: validate_pan(x))


df = pd.read_csv("../csvs/rta/kfintech239.csv")
print(df.columns)
#
# Validation of PAN columns
df["Pan1"] = process_pans(df["Pan1"])
df["Pan2"] = process_pans(df["Pan2"])
df["Pan3"] = process_pans(df["Pan3"])
df["PanG"] = process_pans(df["PanG"])

# Uppercase all names
df["Hold1"] = df["Hold1"].apply(lambda x: x.upper() if isinstance(x, str) else None)
df["Hold2"] = df["Hold2"].apply(lambda x: x.upper() if isinstance(x, str) else None)
df["Hold3"] = df["Hold3"].apply(lambda x: x.upper() if isinstance(x, str) else None)
df["HoldG"] = df["HoldG"].apply(lambda x: x.upper() if isinstance(x, str) else None)

list_of_columns_name = ["fund_code", "folio_number", "holder1_name", "holder2_name", "holder3_name", "guardian_name",
                        "holder1_pan", "holder2_pan", "holder3_pan", "guardian_pan", "holder1_kyc_status",
                        "holder2_kyc_status", "holder3_kyc_status", "guardian_kyc_status"]

updated_df = df[
    ['Fund', 'Acno', 'Hold1', 'Hold2', 'Hold3', 'HoldG', 'Pan1', 'Pan2', 'Pan3',
     'PanG', 'Kyc1', 'Kyc2', 'Kyc3', 'KycG']].rename(columns={
    'Fund': 'fund_code',
    'Acno': 'folio_number',
    'Hold1': 'holder1_name',
    'Hold2': 'holder2_name',
    'Hold3': 'holder3_name',
    'HoldG': 'guardian_name',
    'Pan1': 'holder1_pan',
    'Pan2': 'holder2_pan',
    'Pan3': 'holder3_pan',
    'PanG': 'guardian_pan',
    'Kyc1': 'holder1_kyc_status',
    'Kyc2': 'holder2_kyc_status',
    'Kyc3': 'holder3_kyc_status',
    'KycG': 'guardian_kyc_status'
})
table_name = 'client_details.folio_details'
values = list(updated_df.itertuples(index=False, name=None))
print(updated_df.columns)

perform_upsert(table_name, updated_df.columns, values, 'folio_number')
