"""
Simple configuration file - all settings in one place
"""

# API Endpoints
FRESHSERVICE_URL = "https://m-kopaservicedesk.freshservice.com/api/v2"
SYNAPSE_SERVER = "mk-prd-we-ap-synapse.sql.azuresynapse.net"
SYNAPSE_DATABASE = "AnalyticsDW"

# Claude Settings
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"
MAX_TOKENS = 2000
TEMPERATURE = 0.3

# Fraud Thresholds
FRAUD_SCORE_CRITICAL = 0.70
TAMPER_SCORE_CRITICAL = 0.90
ZERO_CREDIT_DAYS_EVASION = 30

# DFRS Supported Devices
DFRS_DEVICES = [
    "M-KOPA X2", "M-KOPA X20", "M-KOPA X3", "M-KOPA X30",
    "M-KOPA S34", "M-KOPA M10", "M-KOPA 6", "M-KOPA 6000"
]

# Freshservice Field IDs
FRAUD_GROUP_ID = 27000198468
FRAUD_DEPARTMENT_ID = 27000279665

# Database
CACHE_DB = "investigations.db"