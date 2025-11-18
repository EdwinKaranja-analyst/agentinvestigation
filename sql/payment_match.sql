DECLARE @AccountId UNIQUEIDENTIFIER = ?

WITH account_metrics AS (
    SELECT 
        fp.AccountId,
        
        -- Payment behavior metrics
        COUNT(*) AS total_payments,
        COUNT(DISTINCT fp.MaskedSourcePhoneNumber) AS distinct_masked_phones,
        COUNT(DISTINCT fp.PaidBy) AS distinct_payer_names,
        COUNT(DISTINCT fp.Provider) AS distinct_providers,
        COUNT(DISTINCT fp.PaymentChannel) AS distinct_channels,
        
        -- Matching patterns
        SUM(CASE WHEN fp.PhoneMatchingWithCustomerRecordOutcome = 'Matches exactly' THEN 1 ELSE 0 END) AS phone_match_count,
        SUM(CASE WHEN fp.NameMatchingWithCustomerRecordOutcome = 'Matches exactly' THEN 1 ELSE 0 END) AS name_match_count,
        
        -- Self-payment ratios
        CAST(SUM(CASE WHEN fp.PhoneMatchingWithCustomerRecordOutcome = 'Matches exactly' THEN 1 ELSE 0 END) AS FLOAT) 
            / COUNT(*) AS phone_match_ratio,
        CAST(SUM(CASE WHEN fp.NameMatchingWithCustomerRecordOutcome = 'Matches exactly' THEN 1 ELSE 0 END) AS FLOAT) 
            / COUNT(*) AS name_match_ratio,
        
        -- Fraud lead variables
        MAX(leads.LoanCollectionSpeed) AS LoanCollectionSpeed,
        MAX(leads.Model) AS Model,
        MAX(leads.Brand) AS Brand,
        MAX(leads.LoanAge) AS LoanAge,
        MAX(leads.AccountNumber) AS AccountNumber,
        MAX(leads.TotalDeviceForensicsFlagsToDate) AS TotalDeviceForensicsFlagsToDate,
        MAX(leads.TotalFlagstoDate) AS TotalFlagstoDate,
        MAX(leads.CurrentTamperScore) AS CurrentTamperScore,
        MAX(leads.CurrentTamperReason) AS CurrentTamperReason,
        MAX(leads.LifetimeLossRateForecast) AS LifetimeLossRateForecast,
        MAX(leads.ZeroCreditDaysConsecutive) AS ZeroCreditDaysConsecutive
            
    FROM dimensional.fact_payments fp
    LEFT JOIN (
        SELECT
            fld.AccountId,
            loans.LoanCollectionSpeed,
            Model,
            Brand,
            fld.LoanAge,
            fld.AccountNumber,
            TotalDeviceForensicsFlagsToDate,
            TotalFlagstoDate, 
            CurrentTamperScore,
            CurrentTamperReason,
            fld.LifetimeLossRateForecast,
            ZeroCreditDaysConsecutive
        FROM dimensional.fact_fraud_leads AS leads
        JOIN dimensional.dim_loans AS loans ON loans.LoanId = leads.LoanId
        LEFT JOIN dimensional.fact_loans_daily AS fld ON 
            fld.LoanId = loans.LoanId
            AND fld.SnapshotDate = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)
    ) AS leads ON leads.AccountId = fp.AccountId
    
    WHERE fp.PaymentStatus = 1
        AND fp.AccountId = @AccountId
        
    GROUP BY fp.AccountId
)

SELECT 
    *,
    
    -- KYC FRAUD LIKELIHOOD SCORING
    CASE 
        -- HIGH RISK (Score 3)
        WHEN name_match_ratio = 0 AND distinct_masked_phones >= 10 THEN 'HIGH RISK'
        WHEN name_match_ratio < 0.05 AND phone_match_ratio < 0.10 THEN 'HIGH RISK'
        WHEN name_match_ratio = 0 AND distinct_payer_names >= 10 THEN 'HIGH RISK'
        
        -- MEDIUM-HIGH RISK (Score 2)
        WHEN name_match_ratio < 0.10 AND distinct_masked_phones >= 5 THEN 'MEDIUM-HIGH RISK'
        WHEN name_match_ratio = 0 THEN 'MEDIUM-HIGH RISK'
        WHEN name_match_ratio < 0.15 AND phone_match_ratio < 0.15 THEN 'MEDIUM-HIGH RISK'
        
        -- MEDIUM RISK (Score 1)
        WHEN name_match_ratio < 0.30 AND phone_match_ratio < 0.30 THEN 'MEDIUM RISK'
        WHEN distinct_masked_phones >= 15 AND name_match_ratio < 0.50 THEN 'MEDIUM RISK'
        
        -- LOW RISK (Score 0)
        ELSE 'LOW RISK'
    END AS kyc_fraud_likelihood,
    
    -- Individual flags for investigation
    CASE WHEN name_match_ratio = 0 THEN 1 ELSE 0 END AS flag_zero_name_matches,
    CASE WHEN name_match_ratio < 0.10 THEN 1 ELSE 0 END AS flag_very_low_name_match,
    CASE WHEN phone_match_ratio < 0.10 THEN 1 ELSE 0 END AS flag_very_low_phone_match,
    CASE WHEN distinct_masked_phones >= 20 THEN 1 ELSE 0 END AS flag_high_phone_diversity,
    CASE WHEN distinct_payer_names >= 20 THEN 1 ELSE 0 END AS flag_high_name_diversity,
    
    -- Numeric risk score (0-100)
    CAST(
        (CASE WHEN name_match_ratio = 0 THEN 40 ELSE 0 END) +
        (CASE WHEN name_match_ratio < 0.10 THEN 25 ELSE 0 END) +
        (CASE WHEN phone_match_ratio < 0.10 THEN 15 ELSE 0 END) +
        (CASE WHEN distinct_masked_phones >= 20 THEN 10 ELSE 0 END) +
        (CASE WHEN distinct_payer_names >= 20 THEN 10 ELSE 0 END)
    AS INT) AS kyc_fraud_risk_score,
    
    -- Investigation notes
    CASE 
        WHEN name_match_ratio = 0 AND phone_match_ratio > 0.80 
            THEN 'Phone matches but name never matches - likely wrong name registration'
        WHEN name_match_ratio = 0 
            THEN 'Customer NEVER pays under registered name - investigate KYC documents'
        WHEN name_match_ratio < 0.10 AND distinct_masked_phones >= 10 
            THEN 'Extremely low self-payment with high phone diversity - likely aggregator/agent payments'
        WHEN phone_match_ratio < 0.10 AND name_match_ratio < 0.10 
            THEN 'Both phone and name rarely match - systematic third-party payments'
        ELSE 'Review payment patterns for anomalies'
    END AS investigation_note

FROM account_metrics;