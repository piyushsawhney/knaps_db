import pandas as pd

from postgres import perform_upsert
from utils import validate_pan


def process_pans(pan_column):
    return pan_column.apply(lambda x: validate_pan(x))


def update_kfintech_investor_kyc_status():
    df = pd.read_csv("data/rta/kfintech239.csv")
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

    # Uppercase all Status
    df["Kyc1"] = df["Kyc1"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["Kyc2"] = df["Kyc2"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["Kyc3"] = df["Kyc3"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["KycG"] = df["KycG"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)



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
    perform_upsert(table_name, updated_df.columns, values, 'folio_number')
    print("Updated Name, Pan and KYC status for Kfintech Investors")

