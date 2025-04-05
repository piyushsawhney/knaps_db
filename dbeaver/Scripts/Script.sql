SELECT folio_number, fund_code, folio_aum,holder1_name, holder2_name, holder3_name, guardian_name, holder1_pan, holder2_pan, holder3_pan, guardian_pan, holder1_kyc_status, holder1_kyc_desc, holder2_kyc_status, holder2_kyc_desc, holder3_kyc_status, holder3_kyc_desc, guardian_kyc_status, guardian_kyc_desc
FROM client_details.folio_details
WHERE 
((holder1_kyc_status != 'KYC OK' AND holder1_kyc_status != 'VERIFIED') OR
(holder2_kyc_status != 'KYC OK' AND holder2_kyc_status != 'VERIFIED') OR
(holder3_kyc_status != 'KYC OK' AND holder3_kyc_status != 'VERIFIED') OR
(guardian_kyc_status != 'KYC OK' AND guardian_kyc_status != 'VERIFIED')) AND
folio_aum > 1;
