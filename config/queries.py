"""
M-KOPA Fraud Investigation - SQL Queries
All database queries organized by stage

EDIT THIS FILE to:
- Optimize query performance
- Add new data sources
- Adjust time windows
- Modify field selections

Last Updated: 2025-10-30
Version: v1.0
"""

# ============================================================================
# STAGE 1-2: BASIC ACCOUNT LOOKUP (ALWAYS EXECUTE)
# ============================================================================

SQL_STAGE_1_2_ACCOUNT = """
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @LoanID VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?
DECLARE @DeviceID VARCHAR(50) = ?
DECLARE @CustomerID VARCHAR(50) = ?
DECLARE @AccountID VARCHAR(50) = ?
DECLARE @ticketdate DATE = ?

SELECT TOP 1
    dl.LoanID,
    dl.AccountID,
    dl.AccountNumber,
    dl.CustomerID,
    dl.FinancedDeviceID as DeviceID,
    dd.IMEI,
    dd.ModelName as BrandModel,
    dl.SystemLoanStatus,
    dl.LoanStatus,
    dl.ProductCategory,
    dl.ProductSubCategory,
    dl.FulfillmentDate,
    dl.ActivationDate,
    dl.PrincipalAmount,
    CASE 
        WHEN dd.ModelName IN ('M-KOPA X2', 'M-KOPA X20', 'M-KOPA X3', 'M-KOPA X30', 
                              'M-KOPA S34', 'M-KOPA M10', 'M-KOPA 6', 'M-KOPA 6000')
        THEN 1 
        ELSE 0 
    END as SupportsDFRS
FROM dimensional.dim_loans dl WITH (NOLOCK)
INNER JOIN dimensional.dim_devices dd WITH (NOLOCK)
    ON dl.FinancedDeviceID = dd.DeviceID
WHERE 
    (NULLIF(@IMEI, '') IS NOT NULL AND dd.IMEI = @IMEI)
    OR (NULLIF(@LoanID, '') IS NOT NULL AND dl.LoanID = @LoanID)
    OR (NULLIF(@AccountNumber, '') IS NOT NULL AND dl.AccountNumber = @AccountNumber)
    OR (NULLIF(@DeviceID, '') IS NOT NULL AND dl.FinancedDeviceID = @DeviceID)
    OR (NULLIF(@CustomerID, '') IS NOT NULL AND dl.CustomerID = @CustomerID)
    OR (NULLIF(@AccountID, '') IS NOT NULL AND CAST(dl.AccountID AS VARCHAR(50)) = @AccountID)
ORDER BY dl.FulfillmentDate DESC
"""

# ============================================================================
# STAGE 3: DFRS (DEVICE FRAUD RISK SIGNALS) - CONDITIONAL
# ============================================================================

SQL_STAGE_3_DFRS = """
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @LoanID VARCHAR(50) = ?
DECLARE @DeviceID VARCHAR(50) = ?

SELECT 
    SnapShotDate,
    LoanID,
    AccountID,
    IMEI,
    LoanAge,
    ModelName,
    CASE
        WHEN ModelName NOT IN ('M-KOPA X2','M-KOPA X20','M-KOPA X3','M-KOPA X30',
                               'M-KOPA S34','M-KOPA M10','M-KOPA 6','M-KOPA 6000') 
        THEN 'NO DFRS DETAILS'
        ELSE 'VALID DFRS'
    END AS Valid_DFRS,
    ZeroCreditDaysConsecutive,
    ISNULL(FraudScore, 0) AS FraudScore,
    FraudRiskSegment,
    ISNULL(HighestTamperScore, 0) AS HighestTamperScore,
    TamperReason,
    FirstTamperSnapShotDate,
    LastTamperSnapShotDate,
    DATEDIFF(D, FirstTamperSnapShotDate, SnapShotDate) AS DaysSinceFirstTamper,
    DATEDIFF(D, LastTamperSnapShotDate, SnapShotDate) AS DaysSinceLastTamper
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY LoanID ORDER BY SnapShotDate DESC) AS MostRecentSnapShot
    FROM dimensional.fact_loan_fraud_indicators_daily WITH (NOLOCK)
    WHERE (IMEI = @IMEI OR LoanID = @LoanID OR FinancedDeviceID = @DeviceID)
      AND SnapShotDate >= DATEADD(D, -10, GETDATE())
) X
WHERE MostRecentSnapShot = 1
"""

# ============================================================================
# STAGE 4: HISTORICAL FRESHDESK TICKETS - USUALLY EXECUTE
# ============================================================================

SQL_STAGE_4_HISTORICAL = """
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?
DECLARE @AccountID VARCHAR(50) = ?

SELECT TOP 10
    TicketId,
    Subject,
    description,
    Status,
    Priority,
    CreatedTime,
    LastUpdatedTime,
    ReasonForInteraction,
    SubReasonForInteraction,
    AccountId,
    ROW_NUMBER() OVER (ORDER BY CreatedTime DESC) AS TicketSequence
FROM [base_freshdesk].[base_freshdesk_api_tickets] WITH (NOLOCK)
WHERE (DeviceIMEI = @IMEI
    OR AccountNumber = @AccountNumber
    OR CAST(AccountID AS VARCHAR(50)) = @AccountID)
ORDER BY CreatedTime DESC
"""

# ============================================================================
# STAGE 5: BEHAVIORAL ANALYSIS - RARELY EXECUTE (EXPENSIVE!)
# ============================================================================

# NOTE: This is a simplified placeholder
# TODO: Replace with full behavioral query when ready
SQL_STAGE_5_BEHAVIORAL = """
DECLARE @AccountNumber VARCHAR(50) = ?

SELECT 
    @AccountNumber AS AccountNumber,
    0 AS CashLoanTaken,
    NULL AS FulfilledDateCashLoan,
    NULL AS FulfilledCashLoanAmount,
    0 AS ResetPinFromNewDevice,
    NULL AS DaysSinceLastResetPin,
    0 AS MultipleDevicesIncludingSuspicious,
    NULL AS DaysOfMultipleDeviceLogins,
    0 AS UniqueInstallations
"""

# ============================================================================
# QUERY VARIATIONS (For Future Experimentation)
# ============================================================================

# Alternative: Get more historical tickets
SQL_HISTORICAL_TOP_20 = """
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?
DECLARE @AccountID VARCHAR(50) = ?

SELECT TOP 20
    TicketId,
    Subject,
    description,
    Status,
    Priority,
    CreatedTime,
    LastUpdatedTime,
    ReasonForInteraction,
    SubReasonForInteraction,
    AccountId,
    ROW_NUMBER() OVER (ORDER BY CreatedTime DESC) AS TicketSequence
FROM [base_freshdesk].[base_freshdesk_api_tickets] WITH (NOLOCK)
WHERE (DeviceIMEI = @IMEI
    OR AccountNumber = @AccountNumber
    OR CAST(AccountID AS VARCHAR(50)) = @AccountID)
ORDER BY CreatedTime DESC
"""

# Alternative: Only fraud-related tickets
SQL_HISTORICAL_FRAUD_ONLY = """
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?
DECLARE @AccountID VARCHAR(50) = ?

SELECT TOP 10
    TicketId,
    Subject,
    description,
    Status,
    Priority,
    CreatedTime,
    LastUpdatedTime,
    ReasonForInteraction,
    SubReasonForInteraction,
    AccountId,
    ROW_NUMBER() OVER (ORDER BY CreatedTime DESC) AS TicketSequence
FROM [base_freshdesk].[base_freshdesk_api_tickets] WITH (NOLOCK)
WHERE (DeviceIMEI = @IMEI
    OR AccountNumber = @AccountNumber
    OR CAST(AccountID AS VARCHAR(50)) = @AccountID)
AND ReasonForInteraction LIKE '%Fraud%'
ORDER BY CreatedTime DESC
"""

# Alternative: Recent tickets only (last 90 days)
SQL_HISTORICAL_RECENT_ONLY = """
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?
DECLARE @AccountID VARCHAR(50) = ?

SELECT TOP 10
    TicketId,
    Subject,
    description,
    Status,
    Priority,
    CreatedTime,
    LastUpdatedTime,
    ReasonForInteraction,
    SubReasonForInteraction,
    AccountId,
    ROW_NUMBER() OVER (ORDER BY CreatedTime DESC) AS TicketSequence
FROM [base_freshdesk].[base_freshdesk_api_tickets] WITH (NOLOCK)
WHERE (DeviceIMEI = @IMEI
    OR AccountNumber = @AccountNumber
    OR CAST(AccountID AS VARCHAR(50)) = @AccountID)
AND CreatedTime >= DATEADD(DAY, -90, GETDATE())
ORDER BY CreatedTime DESC
"""

# ============================================================================
# ORGANIZED QUERY DICTIONARY
# ============================================================================

SQL_QUERIES = {
    # Core queries (used by system)
    'basic_account': SQL_STAGE_1_2_ACCOUNT,
    'dfrs_signals': SQL_STAGE_3_DFRS,
    'historical_tickets': SQL_STAGE_4_HISTORICAL,
    'behavioral_analysis': SQL_STAGE_5_BEHAVIORAL,
    
    # Alternative queries (for experimentation)
    'historical_top_20': SQL_HISTORICAL_TOP_20,
    'historical_fraud_only': SQL_HISTORICAL_FRAUD_ONLY,
    'historical_recent_only': SQL_HISTORICAL_RECENT_ONLY,
}

# ============================================================================
# QUERY BUILDER FUNCTIONS (For Dynamic Queries)
# ============================================================================

def build_historical_query(top_n=10, fraud_only=False, days_back=None):
    """
    Build historical tickets query with custom parameters
    
    Args:
        top_n: Number of tickets to retrieve (default 10)
        fraud_only: Only fraud-related tickets (default False)
        days_back: Only tickets from last N days (default None = all time)
    
    Returns:
        SQL query string
    """
    
    query = f"""
DECLARE @IMEI VARCHAR(50) = ?
DECLARE @AccountNumber VARCHAR(50) = ?
DECLARE @AccountID VARCHAR(50) = ?

SELECT TOP {top_n}
    TicketId,
    Subject,
    description,
    Status,
    Priority,
    CreatedTime,
    LastUpdatedTime,
    ReasonForInteraction,
    SubReasonForInteraction,
    AccountId,
    ROW_NUMBER() OVER (ORDER BY CreatedTime DESC) AS TicketSequence
FROM [base_freshdesk].[base_freshdesk_api_tickets] WITH (NOLOCK)
WHERE (DeviceIMEI = @IMEI
    OR AccountNumber = @AccountNumber
    OR CAST(AccountID AS VARCHAR(50)) = @AccountID)
"""
    
    if fraud_only:
        query += "\nAND ReasonForInteraction LIKE '%Fraud%'"
    
    if days_back:
        query += f"\nAND CreatedTime >= DATEADD(DAY, -{days_back}, GETDATE())"
    
    query += "\nORDER BY CreatedTime DESC"
    
    return query


# ============================================================================
# NOTES FOR SQL OPTIMIZATION
# ============================================================================

"""
PERFORMANCE TIPS:
1. Stage 1-2: Fast (~100ms) - Always needed
2. Stage 3: Medium (~200ms) - Only for DFRS devices
3. Stage 4: Fast (~50ms) - Cheap, run often
4. Stage 5: SLOW (2-5 seconds!) - Only for account takeover

OPTIMIZATION IDEAS:
- Add indexes on DeviceIMEI, AccountNumber in base_freshdesk table
- Cache DFRS results for devices checked recently
- Limit historical ticket lookback window (currently unlimited)
- Consider partitioning large tables by date

FUTURE QUERIES TO ADD:
- Payment history analysis
- Geographic location patterns
- Device swap history
- Suspect network mapping
"""
