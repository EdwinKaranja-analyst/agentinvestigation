-- Account Lookup Query
-- Finds account by IMEI, Phone Number, or Account Number

DECLARE @IMEI VARCHAR(50) = ?
DECLARE @PhoneNumber VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?

SELECT TOP 1
    loans.AccountNumber,
    cust.IdNumber,
    cust.CustomerId,
    cust.PhoneNumber,
    loans.LoanId,
    devices.Imei AS IMEI,
    devices.ModelName,
    devices.BrandModel,
    CASE 
        WHEN devices.ModelName IN ('M-KOPA X2', 'M-KOPA X20', 'M-KOPA X3', 'M-KOPA X30',
                                    'M-KOPA S34', 'M-KOPA M10', 'M-KOPA 6', 'M-KOPA 6000')
        THEN 1 
        ELSE 0 
    END AS SupportsDFRS
FROM dimensional.dim_loans loans
INNER JOIN dimensional.dim_customers AS cust
    ON cust.CustomerId = loans.CustomerId
INNER JOIN dimensional.dim_devices AS devices
    ON devices.DeviceId = loans.CollateralDeviceId
WHERE 
    devices.Imei = @IMEI
    OR cust.PhoneNumber = @PhoneNumber
    OR loans.AccountNumber = @AccountNumber
ORDER BY loans.LoanCreatedDate DESC