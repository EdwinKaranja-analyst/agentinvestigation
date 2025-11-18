    SELECT 
        InstallationID,
        COUNT(DISTINCT AccountIdOrPhone) as AccountsLogin,
        COUNT(*) as TotalLogins
    FROM [raw_datalake].[EventLoginSuccessful] WITH (NOLOCK)
    WHERE TRY_CONVERT(datetime2, OccurredAt) IS NOT NULL
        AND AccountType = 'Customer'
        AND InstallationID IS NOT NULL
        AND TRY_CONVERT(datetime2, OccurredAt) > DATEADD(M,-8,GETDATE())
    GROUP BY InstallationID
    HAVING COUNT(DISTINCT AccountIdOrPhone) >= 10




SELECT TOP 100*

FROM raw_datalake.[EventLoginSuccessful]



DECLARE @CustomerId UNIQUEIDENTIFIER = '41921778-0c63-f011-909b-000d3a2f4e87';

WITH account_devices AS (
    -- Get all devices this customer has logged in from
    SELECT DISTINCT
        CustomerId,
        DeviceIdentifier
    FROM raw_datalake.EventLoginSuccessful
    WHERE CustomerId = @CustomerId
        AND AccountType = 'Customer'
        AND DeviceIdentifier IS NOT NULL
        AND DeviceIdentifier != '00000000-0000-0000-0000-000000000000'
),
device_account_counts AS (
    -- For each device, count how many different customers use it
    SELECT 
        DeviceIdentifier,
        COUNT(DISTINCT CustomerId) AS total_customers_on_device
    FROM raw_datalake.EventLoginSuccessful
    WHERE AccountType = 'Customer'
        AND DeviceIdentifier IS NOT NULL
        AND DeviceIdentifier != '00000000-0000-0000-0000-000000000000'
    GROUP BY DeviceIdentifier
),
account_device_summary AS (
    -- Combine customer's devices with their sharing metrics
    SELECT 
        ad.CustomerId,
        ad.DeviceIdentifier,
        dac.total_customers_on_device,
        -- Get last login date for this device
        MAX(els.OccurredAt) AS last_login_on_device
    FROM account_devices ad
    INNER JOIN device_account_counts dac ON ad.DeviceIdentifier = dac.DeviceIdentifier
    LEFT JOIN raw_datalake.EventLoginSuccessful els ON 
        els.DeviceIdentifier = ad.DeviceIdentifier 
        AND els.CustomerId = ad.CustomerId
        AND els.AccountType = 'Customer'
    GROUP BY ad.CustomerId, ad.DeviceIdentifier, dac.total_customers_on_device
),
summary_metrics AS (
    SELECT 
        @CustomerId AS CustomerId,
        
        -- Device diversity metrics
        COUNT(DISTINCT DeviceIdentifier) AS unique_devices_used,
        
        -- Device sharing risk metrics
        MAX(total_customers_on_device) AS max_customers_per_device,
        AVG(CAST(total_customers_on_device AS FLOAT)) AS avg_customers_per_device,
        SUM(CASE WHEN total_customers_on_device >= 5 THEN 1 ELSE 0 END) AS high_risk_devices_count,
        SUM(CASE WHEN total_customers_on_device >= 3 THEN 1 ELSE 0 END) AS medium_risk_devices_count,
        
        -- Most recent login
        MAX(last_login_on_device) AS last_login_date,
        
        -- Risk scoring
        CASE 
            -- CRITICAL RISK
            WHEN MAX(total_customers_on_device) >= 10 THEN 'CRITICAL - Device used by 10+ customers'
            WHEN COUNT(DISTINCT DeviceIdentifier) >= 10 THEN 'CRITICAL - Customer logged from 10+ devices'
            
            -- HIGH RISK  
            WHEN MAX(total_customers_on_device) >= 5 THEN 'HIGH - Device used by 5+ customers'
            WHEN COUNT(DISTINCT DeviceIdentifier) >= 5 THEN 'HIGH - Customer logged from 5+ devices'
            
            -- MEDIUM RISK
            WHEN MAX(total_customers_on_device) >= 3 THEN 'MEDIUM - Device used by 3+ customers'
            WHEN COUNT(DISTINCT DeviceIdentifier) >= 3 THEN 'MEDIUM - Customer logged from 3+ devices'
            
            -- LOW RISK
            ELSE 'LOW - Normal device usage'
        END AS device_risk_level,
        
        -- Risk score (0-100)
        CAST(
            (CASE WHEN MAX(total_customers_on_device) >= 10 THEN 50
                  WHEN MAX(total_customers_on_device) >= 5 THEN 35
                  WHEN MAX(total_customers_on_device) >= 3 THEN 20
                  ELSE 0 END) +
            (CASE WHEN COUNT(DISTINCT DeviceIdentifier) >= 10 THEN 50
                  WHEN COUNT(DISTINCT DeviceIdentifier) >= 5 THEN 35
                  WHEN COUNT(DISTINCT DeviceIdentifier) >= 3 THEN 20
                  ELSE 0 END)
        AS INT) AS device_risk_score
    
    FROM account_device_summary
    GROUP BY CustomerId
)

-- Combined output with summary and device details
SELECT 
    sm.CustomerId,
    sm.unique_devices_used,
    sm.max_customers_per_device,
    sm.avg_customers_per_device,
    sm.high_risk_devices_count,
    sm.medium_risk_devices_count,
    sm.last_login_date,
    sm.device_risk_level,
    sm.device_risk_score,
    
    -- Device details
    ads.DeviceIdentifier,
    ads.total_customers_on_device,
    ads.last_login_on_device,
    CASE 
        WHEN ads.total_customers_on_device >= 10 THEN 'CRITICAL'
        WHEN ads.total_customers_on_device >= 5 THEN 'HIGH'
        WHEN ads.total_customers_on_device >= 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS device_risk_flag

FROM summary_metrics sm
LEFT JOIN account_device_summary ads ON sm.CustomerId = ads.CustomerId
ORDER BY ads.total_customers_on_device DESC, ads.last_login_on_device DESC;