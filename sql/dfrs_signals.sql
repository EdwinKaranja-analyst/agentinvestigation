-- DFRS Query - Device Fraud Risk Signals
-- Only run for devices that support DFRS

DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?

SELECT
	fld.IMEI,
	fld.SnapshotDate, --Details captured at this date
	fld.CountryCode, --The country the account belongs to
	fld.ModelName, --Device Model Name
	fld.SystemLoanStatus, --Payment Loan Status
	fld.LoanStatus, --Payment Loan Status
	fld.AmountPaidToDate, 
	fld.LoanAge,
	fld.ZeroCreditDaysToDate, --Number of days account has stayed without credit. 
	fld.ZeroCreditDaysConsecutive,
	fld.CumulativeDaysInArrears,
	fld.LoanCollectionSpeed, --Collection speed. Ideal is 1 which represents 100%
	-- dim_devices.SecurityStatus,
	fld.PhoneState, 
	fld.LastUsagePickedAt,
	fld.LastSeenAt, --Last time the device checked in according to HMD data
	fld.DaysSinceLastSeen,
	fld.DaysSinceLastUsagePicked,
	fld.LastHeartbeatDate, --Last date the device checked in based on Kilpitek data. 
	fld.TotalHeartbeatsL7,     --Number of times the device checked in.
	fld.FraudScore,
	fld.FraudRiskSegment,
	fld.TamperScore, 
	fld.TamperReason, 
	fld.DaysBetweenLastHeartbeatAndFirstTamper,
	fld.FirmwareVersion,
	fld.ClientVersion,
	fld.ChipsetVersion,
	fld.SecurityStatus,
	fld.FinancedDeviceId,
	fld.LoanId,
	fld.AccountId,
	fld.CustomerId,
	fld.PaymentPlanId
 
 
FROM dimensional.fact_loan_fraud_indicators_daily fld 
	INNER JOIN dimensional.dim_loans AS loans
	ON loans.LoanId = fld.LoanId
 
WHERE fld.SnapshotDate>='2025-01-01'
	AND fld.Imei in (@IMEI)
	 OR loans.AccountNumber IN (@AccountNumber)