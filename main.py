from data_processor.cams_wbr56 import update_cams_investor_kyc_status
from data_processor.kfintech_mfsd239 import update_kfintech_investor_kyc_status
from data_processor.redvision_aum_folio import process_folio_with_aum

if __name__ == '__main__':
    # First Update AUM Folios
    # Second Run Kfintech MFSD 240 For Folio Details (not for now)
    # Kfintech MFSD 239 only for Names, PANs,  KYC status
    # CAMS WBR262 for folio details as well KYC status (Valid and Not Valid)
    process_folio_with_aum()
    update_kfintech_investor_kyc_status()
    update_cams_investor_kyc_status("data/rta/camswbr56_non_validated.csv")
    # update_cams_investor_kyc_status("data/rta/camswbr56_validated.csv")
