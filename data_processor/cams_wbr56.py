import pandas as pd

from postgres import perform_upsert
from utils import validate_pan


def process_pans(pan_column):
    return pan_column.apply(lambda x: validate_pan(x))


def update_cams_investor_kyc_status(file_path):
    df = pd.read_csv(file_path, quotechar="'")
    #
    # Validation of PAN columns
    df["TAX_NO"] = process_pans(df["TAX_NO"])
    df["JOINTPAN1"] = process_pans(df["JOINTPAN1"])
    df["JOINTPAN2"] = process_pans(df["JOINTPAN2"])
    df["GUARDIAN_PANNO"] = process_pans(df["GUARDIAN_PANNO"])

    # Uppercase all names
    df["INV_NAME"] = df["INV_NAME"].apply(lambda x: x.upper() if isinstance(x, str) else None)
    df["JNAME1"] = df["JNAME1"].apply(lambda x: x.upper() if isinstance(x, str) else None)
    df["JNAME2"] = df["JNAME2"].apply(lambda x: x.upper() if isinstance(x, str) else None)
    df["GUARDIAN"] = df["GUARDIAN"].apply(lambda x: x.upper() if isinstance(x, str) else None)

    # Uppercase all status
    df["FH_KYC"] = df["FH_KYC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["JH1_KYC"] = df["JH1_KYC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["JH2_KYC"] = df["JH2_KYC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["GU_KYC"] = df["GU_KYC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)

    # Uppercase all description
    df["FH_KYC_DESC"] = df["FH_KYC_DESC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["JH1_KYC_DESC"] = df["JH1_KYC_DESC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["JH2_KYC_DESC"] = df["JH2_KYC_DESC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)
    df["GU_KYC_DESC"] = df["GU_KYC_DESC"].apply(lambda x: x.upper() if isinstance(x, str) and len(x) > 2 else None)

    updated_df = df[
        ['FOLIO', 'INV_NAME', 'JNAME1', 'JNAME2', 'GUARDIAN', 'TAX_NO', 'JOINTPAN1', 'JOINTPAN2',
         'GUARDIAN_PANNO', 'FH_KYC', 'JH1_KYC', 'JH2_KYC', 'GU_KYC', 'FH_KYC_DESC', 'JH1_KYC_DESC', 'JH2_KYC_DESC',
         'GU_KYC_DESC']].rename(columns={
        'FOLIO': 'folio_number',
        'INV_NAME': 'holder1_name',
        'JNAME1': 'holder2_name',
        'JNAME2': 'holder3_name',
        'GUARDIAN': 'guardian_name',
        'TAX_NO': 'holder1_pan',
        'JOINTPAN1': 'holder2_pan',
        'JOINTPAN2': 'holder3_pan',
        'GUARDIAN_PANNO': 'guardian_pan',
        'FH_KYC': 'holder1_kyc_status',
        'JH1_KYC': 'holder2_kyc_status',
        'JH2_KYC': 'holder3_kyc_status',
        'GU_KYC': 'guardian_kyc_status',
        'FH_KYC_DESC': 'holder1_kyc_desc',
        'JH1_KYC_DESC': 'holder2_kyc_desc',
        'JH2_KYC_DESC': 'holder3_kyc_desc',
        'GU_KYC_DESC': 'guardian_kyc_desc'

    })
    table_name = 'client_details.folio_details'
    values = list(updated_df.itertuples(index=False, name=None))
    perform_upsert(table_name, updated_df.columns, values, 'folio_number')
    print("Updated Name, Pan and KYC status for CAMS Investors")
