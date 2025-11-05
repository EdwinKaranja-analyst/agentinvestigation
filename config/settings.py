"""
M-KOPA Fraud Investigation - Settings & Constants
Edit this file to adjust thresholds, mappings, and configuration

Last Updated: 2025-10-30
Version: v1.0
"""

# ============================================================================
# API ENDPOINTS
# ============================================================================

FRESHSERVICE_BASE_URL = "https://m-kopaservicedesk.freshservice.com/api/v2"
SYNAPSE_SERVER = 'mk-prd-we-ap-synapse.sql.azuresynapse.net'
SYNAPSE_DATABASE = 'AnalyticsDW'

# ============================================================================
# CLAUDE AI CONFIGURATION
# ============================================================================

CLAUDE_MODEL = "claude-sonnet-4-5-20250929"
CLAUDE_MAX_TOKENS_PLANNING = 1500
CLAUDE_MAX_TOKENS_INVESTIGATION = 2500
CLAUDE_TEMPERATURE_PLANNING = 0.2
CLAUDE_TEMPERATURE_INVESTIGATION = 0.3

# ============================================================================
# FRAUD DETECTION THRESHOLDS (TUNE THESE!)
# ============================================================================

THRESHOLDS = {
    # DFRS Scores
    'fraud_score_critical': 0.70,      # >0.7 = HIGH RISK
    'fraud_score_warning': 0.50,       # >0.5 = MEDIUM RISK
    'tamper_score_critical': 0.90,     # >0.9 = CONFIRMED TAMPERING
    'tamper_score_warning': 0.60,      # >0.6 = LIKELY TAMPERING
    
    # Payment Patterns
    'zero_credit_days_evasion': 30,    # >30 days = PAYMENT EVASION
    
    # Confidence Levels
    'confidence_high': 0.70,           # >70% = Awaiting field Investigation
    'confidence_medium': 0.55,         # 55-70% = Re-investigate
    # <55% = Not fraud
    
    # Historical Patterns
    'historical_repeat_offender': 3,   # 3+ fraud tickets = REPEAT OFFENDER
    'historical_recent_days': 90,      # Last 90 days = RECENT
}

# ============================================================================
# DEVICE MODELS (DFRS Support)
# ============================================================================

DFRS_SUPPORTED_MODELS = [
    'M-KOPA X2',
    'M-KOPA X20',
    'M-KOPA X3',
    'M-KOPA X30',
    'M-KOPA S34',
    'M-KOPA M10',
    'M-KOPA 6',
    'M-KOPA 6000'
]

# ============================================================================
# FRAUD CATEGORIES & MAPPINGS
# ============================================================================

# Valid fraud allegation options
FRAUD_ALLEGATION_OPTIONS = [
    "Cash Payments",
    "Hacking & Tampering",
    "Identity Theft",
    "Cash Loan Fraud",
    "Hardware theft (Lost & Found)",
    "Hardware theft DSR",
    "Resale",
    "Stolen stock"
]

# Allegation to Subreason mapping
ALLEGATION_TO_SUBREASON = {
    "Cash Loan Fraud": "Cash Payments",
    "Cash Payments": "Cash Payments",
    "Identity Theft": "Identity theft",
    "Hacking & Tampering": "Hacking and tampering",
    "Hardware theft (Lost & Found)": "Hardware theft",
    "Hardware theft DSR": "Hardware theft",
    "Resale": "Resale",
    "Stolen stock": "Hardware theft"
}

# Reason standardization (for consistency)
REASON_STANDARDIZATION = {
    "Cash Loan Fraud": "Cash Loan Fraud",
    "Cash loan fraud": "Cash Loan Fraud",
    "Customer Fraud": "Customer Fraud",
    "Customer fraud": "Customer Fraud"
}

# ============================================================================
# FRESHSERVICE FIELD IDs
# ============================================================================

# Fixed values for ticket assignment (Phase 5)
BASIC_FIELD_UPDATES = {
    'group_id': 27000198468,
    'department_id': 27000279665,
    'category': 'Fraud Team',
    'responder_id': 27002721925
}

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

CACHE_DB_PATH = "investigations_cache.db"
RESULTS_DIR = "batch_results"

# ============================================================================
# INVESTIGATION ROUTES
# ============================================================================

# Pre-defined investigation routes (what stages to run)
INVESTIGATION_ROUTES = {
    'device_tampering_standard': {
        'stages': [1, 2, 3, 4],  # Skip behavioral
        'description': "Device tampering check with DFRS"
    },
    'account_takeover_full': {
        'stages': [1, 2, 4, 5],  # Skip DFRS, include behavioral
        'description': "Account takeover with behavioral analysis"
    },
    'cash_loan_comprehensive': {
        'stages': [1, 2, 3, 4, 5],  # All stages
        'description': "Comprehensive check for cash loan fraud"
    },
    'payment_fraud_basic': {
        'stages': [1, 2, 3, 4],  # Skip behavioral
        'description': "Payment fraud check"
    },
    'external_scam_quick': {
        'stages': [1, 2, 4],  # Skip DFRS and behavioral
        'description': "Quick check for external scams"
    },
    'quick_lookup': {
        'stages': [1, 2, 4],  # Minimal
        'description': "Fast lookup for low-priority cases"
    }
}

# ============================================================================
# FRAUD TYPE DEFINITIONS
# ============================================================================

FRAUD_TYPE_DEFINITIONS = {
    'device_tampering': "Lock disabled, IMEI changed, device opened, sensor bypass",
    'account_takeover': "Unauthorized login, PIN reset, hacked account, SIM swap",
    'cash_loan_fraud': "Cash loan issues, unauthorized disbursement",
    'payment_fraud': "Third-party payments, unauthorized transactions",
    'network_fraud': "Organized fraud rings, multiple accounts on one device",
    'external_scam': "Facebook scam, OTP phishing, impersonation",
    'identity_theft': "Fake credentials, wrong KYC",
    'dsr_misconduct': "DSR wrongdoing",
    'unknown': "Cannot determine from ticket"
}
