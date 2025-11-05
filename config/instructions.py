"""
M-KOPA Fraud Investigation - Claude AI Instructions
All prompts, training examples, and AI configuration

EDIT THIS FILE to:
- Add new training examples
- Refine fraud detection logic
- Update classification rules
- Improve prompt wording

Last Updated: 2025-10-30
Version: v1.0
Updated By: Bryan Edwards
"""

import json
from typing import Dict, Any

# ============================================================================
# VERSION TRACKING
# ============================================================================

PROMPT_VERSION = "v1.0"
LAST_UPDATED = "2025-10-30"
CHANGELOG = """
v1.0 (2025-10-30):
- Initial separation from monolithic file
- 5 training examples
- Phase 2 and Phase 4 prompts
"""

# ============================================================================
# TRAINING EXAMPLES (ADD YOUR OWN HERE!)
# ============================================================================

TRAINING_EXAMPLES = [
    {
        "label": "EXTERNAL_SCAM",
        "description": "Facebook scam - fake M-KOPA loan page, customer prevented fraud",
        "signals": "No DFRS, No behavioral, 0 historical tickets",
        "result": "Likely fraud, 92% confidence, Cash Loan Fraud, External suspect"
    },
    {
        "label": "DEVICE_TAMPERING",
        "description": "Lock disabled, device not working properly",
        "signals": "DFRS: FraudScore 0.72, TamperScore 0.85, ZeroCreditDays 45, 2 historical tickets",
        "result": "Likely fraud, 88% confidence, Hacking & Tampering, Customer suspect"
    },
    {
        "label": "ACCOUNT_TAKEOVER",
        "description": "Unauthorized PIN reset, customer didn't request",
        "signals": "No DFRS, Behavioral: ResetPinFromNewDevice=1, LinkedToSuspiciousDevice=1",
        "result": "Likely fraud, 95% confidence, Identity Theft, External suspect, CRITICAL - fraud ring"
    },
    {
        "label": "WRONG_ESCALATION",
        "description": "Manager following up for screening",
        "signals": "DFRS: Low scores, No behavioral flags, Normal account",
        "result": "Not fraud, 92% confidence, No action required"
    },
    {
        "label": "PAYMENT_EVASION",
        "description": "Device issues, customer not paying",
        "signals": "DFRS: TamperScore 0.65, ZeroCreditDays 55, 1 historical ticket",
        "result": "Likely fraud, 78% confidence, Hacking & Tampering, Re-investigate"
    }
]

# ============================================================================
# PHASE 2: QUERY PLANNING PROMPT
# ============================================================================

QUERY_PLANNING_INSTRUCTIONS = """You are a fraud investigation query orchestrator for M-KOPA. Your job is to analyze a ticket and decide which database queries to execute.

CRITICAL: Your goal is to run the MINIMUM queries needed while getting maximum information. Some queries are expensive (2-5 seconds) and should only be run when truly necessary.

AVAILABLE SQL STAGES:

**Stage 1-2: Basic Account Lookup** (ALWAYS EXECUTE)
- Time: ~100ms
- Cost: Low
- Purpose: Get account profile, device info, loan status
- Required for: All investigations

**Stage 3: DFRS (Device Fraud Risk Signals)** (CONDITIONAL)
- Time: ~200ms
- Cost: Medium
- Purpose: Tampering scores, fraud scores, zero-credit days
- Execute when:
  ✓ Device supports DFRS (M-KOPA X2, X20, X3, X30, S34, M10, 6, 6000)
  ✓ Fraud type is device tampering, payment evasion, or unknown
  ✓ Ticket mentions: lock, tamper, device issues, manipulation
- Skip when:
  ✗ Device is Samsung/Nokia (doesn't support DFRS)
  ✗ Pure account takeover (device irrelevant)
  ✗ External scam (no device involvement)

**Stage 4: Historical Freshdesk Tickets** (USUALLY EXECUTE)
- Time: ~50ms
- Cost: Low
- Purpose: Check for repeat offender patterns
- Execute when:
  ✓ Most cases (default YES - it's fast and informative)
- Skip when:
  ✗ Quick lookup only (very low priority)
  ✗ First-time generic inquiry

**Stage 5: Login Behavioral Analysis** (EXPENSIVE - RARELY EXECUTE)
- Time: 2-5 seconds
- Cost: HIGH
- Purpose: Account takeover, fraud ring detection, PIN resets
- Execute when:
  ✓ Fraud type is account_takeover or network_fraud
  ✓ Ticket mentions: unauthorized login, PIN reset, hacked account, multiple devices
  ✓ Cash loan fraud with suspicious timing
- Skip when:
  ✗ Device tampering only
  ✗ External scam (no account access)
  ✗ Payment fraud (device-level only)
  ✗ DEFAULT: NO (too expensive)

FRAUD TYPE CLASSIFICATION:

- **device_tampering**: Lock disabled, IMEI changed, device opened, sensor bypass
- **account_takeover**: Unauthorized login, PIN reset, hacked account, SIM swap
- **cash_loan_fraud**: Cash loan issues, unauthorized disbursement
- **payment_fraud**: Third-party payments, unauthorized transactions
- **network_fraud**: Organized fraud rings, multiple accounts on one device
- **external_scam**: Facebook scam, OTP phishing, impersonation
- **identity_theft**: Fake credentials, wrong KYC
- **dsr_misconduct**: DSR wrongdoing
- **unknown**: Cannot determine from ticket

INVESTIGATION ROUTES:

- **device_tampering_standard**: Stages 1-2, 3, 4 (skip 5)
- **account_takeover_full**: Stages 1-2, 4, 5 (skip 3)
- **cash_loan_comprehensive**: All stages 1-5
- **payment_fraud_basic**: Stages 1-2, 3, 4 (skip 5)
- **external_scam_quick**: Stages 1-2, 4 (skip 3 and 5)
- **quick_lookup**: Stages 1-2, 4 only
"""

QUERY_PLANNING_OUTPUT_FORMAT = """{
  "identifiers_extracted": {
    "IMEI": "..." or null,
    "LoanID": "..." or null,
    "AccountNumber": "..." or null,
    "PhoneNumber": "..." or null,
    "DeviceID": "..." or null,
    "CustomerID": "..." or null,
    "AccountID": "..." or null
  },
  "device_model": "..." or null,
  "supports_dfrs": true/false,
  "fraud_type": "device_tampering" | "account_takeover" | "cash_loan_fraud" | "payment_fraud" | "network_fraud" | "external_scam" | "identity_theft" | "dsr_misconduct" | "unknown",
  "risk_level": "low" | "medium" | "high" | "critical",
  "investigation_route": "device_tampering_standard" | "account_takeover_full" | "cash_loan_comprehensive" | "payment_fraud_basic" | "external_scam_quick" | "quick_lookup",
  "execute_stage_3_dfrs": true/false,
  "execute_stage_4_history": true/false,
  "execute_stage_5_behavioral": true/false,
  "confidence": 0.XX,
  "reasoning": "Explain your decisions",
  "estimated_time_ms": XXX,
  "key_concerns": ["Concern 1", "Concern 2"]
}"""

QUERY_PLANNING_RULES = """RULES:
- Stage 1-2 ALWAYS execute
- Stage 3 only if device supports DFRS AND fraud is device-related
- Stage 4 execute by default (fast) unless pure external scam
- Stage 5 ONLY for account takeover, network fraud, or high-value cash loan
- Be conservative on Stage 5 - when in doubt, skip it"""


def get_query_planning_prompt(freshservice_data: Dict[str, Any]) -> str:
    """
    Generate Phase 2 query planning prompt
    
    Args:
        freshservice_data: Ticket data from Phase 1
    
    Returns:
        Complete prompt for Claude
    """
    
    ticket_id = freshservice_data.get('ticket_id')
    basic_data = freshservice_data.get('basic_data', {})
    custom_fields = basic_data.get('custom_fields', {})
    conversations = freshservice_data.get('conversations', [])
    
    subject = basic_data.get('subject', '')
    description_text = basic_data.get('description_text', '')
    description_html = basic_data.get('description', '')
    case_details = custom_fields.get('case_details', '')
    
    description = description_text or description_html
    
    # Prepare conversation text
    conversation_text = ""
    for i, conv in enumerate(conversations, 1):
        body = conv.get('body_text', '')
        if body:
            conversation_text += f"\nConversation {i}: {body}\n"
    
    # Build complete prompt
    prompt = f"""{QUERY_PLANNING_INSTRUCTIONS}

TICKET TO ANALYZE:

Ticket ID: {ticket_id}
Subject: {subject}
Description: {description[:1000] if description else 'No description'}
Case Details: {case_details[:1000] if case_details else 'No case details'}
Conversations: {conversation_text[:1000] if conversation_text else 'No conversations'}

Your task:
1. Extract ALL identifiers (IMEI, phone, account number, loan ID, etc.)
2. Classify fraud type
3. Determine device model if mentioned (check if supports DFRS)
4. Assess risk level
5. Decide which SQL stages to execute
6. Select investigation route
7. Provide reasoning

Return ONLY valid JSON:

{QUERY_PLANNING_OUTPUT_FORMAT}

{QUERY_PLANNING_RULES}

Analyze the ticket:
"""
    
    return prompt


# ============================================================================
# PHASE 4: FRAUD INVESTIGATION PROMPT
# ============================================================================

INVESTIGATION_INSTRUCTIONS = """You are an expert M-KOPA fraud investigator analyzing case.

You now have ENHANCED data from multiple sources. Use ALL available data to make the best classification.

CRITICAL DOMAIN KNOWLEDGE:

1. DFRS (Device Fraud Risk Signals) - When Available:
   - FraudScore > 0.7 → CRITICAL device fraud indicator
   - TamperScore > 0.9 → Confirmed physical tampering
   - TamperScore > 0.6 → Likely tampering
   - ZeroCreditDaysConsecutive > 30 → Payment evasion pattern
   - Combined: High tamper + high zero days = device manipulation to avoid payment

2. Behavioral Analysis - When Available:
   - ResetPinFromNewDevice = 1 → CRITICAL account takeover indicator
   - MultipleDevicesIncludingSuspicious = 1 → CRITICAL fraud ring connection
   - CashLoanTaken = 1 after PIN reset → High-risk fraud pattern
   - These are VERY strong signals - weight heavily!

3. Historical Patterns:
   - 0 tickets: First time
   - 1-2 tickets: Occasional issues
   - 3+ fraud tickets: Repeat offender pattern
   - Same issue repeatedly: Ongoing problem vs new fraud

4. Statistical Priors (from earlier analysis):
   - Phone mismatch → 2.9x higher KYC risk (from old data)
   - Multiple payers → Third-party fraud indicator

5. Classification Standards:
   - "Likely fraud": Reasonable evidence without physical proof (YOUR PRIMARY OUTPUT)
   - "Not fraud": Insufficient evidence, wrong escalation
   - Never use "Confirmed fraud" (requires physical inspection)"""

INVESTIGATION_OUTPUT_FORMAT = """{
  "fraud_status": "Likely fraud" or "Not fraud",
  "confidence": 0.XX,
  "primary_allegation": "..." or null,
  "suspect_type": "..." or null,
  "suspect_name": "..." or null,
  "suspect_number": "..." or null,
  "case_outcome": "Awaiting field Investigation" | "Re-investigate" | "No action required",
  "investigation_summary": "5 sentences max with facts, data sources, evidence, recommended actions",
  "public_note": "2-3 sentences, generic, NO PII (no account numbers, names, phones, scores)",
  "key_evidence": [
    "Evidence point 1",
    "Evidence point 2",
    "Evidence point 3"
  ],
  "risk_factors": {
    "dfrs_fraud_score": 0.XX or null,
    "dfrs_tamper_score": 0.XX or null,
    "behavioral_account_takeover": true/false,
    "behavioral_fraud_ring": true/false,
    "repeat_offender": true/false,
    "payment_evasion": true/false
  },
  "data_sources_used": {
    "dfrs_available": true/false,
    "behavioral_available": true/false,
    "historical_count": X
  },
  "recommended_next_steps": [
    "Action 1",
    "Action 2"
  ]
}"""

INVESTIGATION_BUSINESS_RULES = """BUSINESS RULES:
- If fraud_status = "Likely fraud" → primary_allegation REQUIRED (from valid list)
- If fraud_status = "Not fraud" → primary_allegation = null
- suspect_type can be inferred even without suspect_number
- Extract phone numbers and add leading 0
- Public note: NO PII (no account numbers, names, phones, scores)"""


def get_investigation_prompt(
    ticket_id: str,
    subject: str,
    case_details: str,
    conversations: str,
    fraud_type: str,
    risk_level: str,
    account_profile: Dict[str, Any],
    dfrs_signals: Dict[str, Any],
    historical_tickets: list,
    behavioral_analysis: Dict[str, Any]
) -> str:
    """
    Generate Phase 4 investigation prompt
    
    Args:
        Various data from Phases 1-3
    
    Returns:
        Complete investigation prompt for Claude
    """
    
    # Format training examples
    examples_text = json.dumps(TRAINING_EXAMPLES, indent=2)
    
    # Format DFRS signals
    dfrs_text = "No DFRS data available"
    if dfrs_signals:
        dfrs_text = f"""
Fraud Score: {dfrs_signals.get('FraudScore', 0):.2f} (0-1 scale, >0.7 is critical)
Tamper Score: {dfrs_signals.get('HighestTamperScore', 0):.2f} (0-1 scale, >0.9 is critical)
Tamper Reason: {dfrs_signals.get('TamperReason', 'None')}
Zero Credit Days Consecutive: {dfrs_signals.get('ZeroCreditDaysConsecutive', 0)} (>30 is payment evasion)
Risk Segment: {dfrs_signals.get('FraudRiskSegment', 'Unknown')}
Days Since Last Tamper: {dfrs_signals.get('DaysSinceLastTamper', 'N/A')}
"""
    
    # Format behavioral analysis
    behavioral_text = "No behavioral data available"
    if behavioral_analysis:
        behavioral_text = f"""
Cash Loan Taken: {behavioral_analysis.get('CashLoanTaken', 0)} (1 = yes, 0 = no)
Cash Loan Amount: KES {behavioral_analysis.get('FulfilledCashLoanAmount', 0)}
PIN Reset from New Device: {behavioral_analysis.get('ResetPinFromNewDevice', 0)} (1 = ACCOUNT TAKEOVER RISK!)
Days Since PIN Reset: {behavioral_analysis.get('DaysSinceLastResetPin', 'N/A')}
Linked to Suspicious Device: {behavioral_analysis.get('MultipleDevicesIncludingSuspicious', 0)} (1 = FRAUD RING!)
Unique Installations: {behavioral_analysis.get('UniqueInstallations', 0)}
"""
    
    # Format historical tickets
    historical_text = f"Found {len(historical_tickets)} previous tickets"
    if historical_tickets:
        historical_text += "\n"
        for i, ticket in enumerate(historical_tickets[:5], 1):
            historical_text += f"\n{i}. {ticket.get('Subject', 'N/A')[:60]}"
            historical_text += f"\n   Created: {ticket.get('CreatedTime', 'N/A')}"
            historical_text += f"\n   Status: {ticket.get('Status', 'N/A')}"
            historical_text += f"\n   Reason: {ticket.get('ReasonForInteraction', 'N/A')}\n"
    
    prompt = f"""{INVESTIGATION_INSTRUCTIONS}

TRAINING EXAMPLES:
{examples_text}

PHASE 2 INITIAL ASSESSMENT (from query planning):
Fraud Type (from ticket text): {fraud_type}
Risk Level: {risk_level}

CASE DATA:

Ticket ID: {ticket_id}
Subject: {subject}
Case Details: {case_details[:1000] if case_details else 'No case details'}
{f'Conversations: {conversations[:500]}' if conversations else 'No conversations'}

ACCOUNT PROFILE (Stage 1-2):
Account: {account_profile.get('AccountNumber', 'Not found')}
Device: {account_profile.get('BrandModel', 'Unknown')}
IMEI: {account_profile.get('IMEI', 'Unknown')}
Loan Status: {account_profile.get('SystemLoanStatus', 'Unknown')}
Product: {account_profile.get('ProductSubCategory', 'Unknown')}

DFRS SIGNALS (Stage 3):
{dfrs_text}

BEHAVIORAL ANALYSIS (Stage 5):
{behavioral_text}

HISTORICAL TICKETS (Stage 4):
{historical_text}

ANALYSIS INSTRUCTIONS:

1. Evaluate DFRS signals (if available):
   - FraudScore > 0.7 = strong fraud indicator
   - TamperScore > 0.9 = confirmed tampering
   - ZeroCreditDays > 30 = payment evasion

2. Check behavioral red flags (if available):
   - ResetPinFromNewDevice = 1 = CRITICAL account takeover
   - Linked to suspicious device = CRITICAL fraud ring
   - Weight these VERY heavily!

3. Review historical patterns:
   - Count fraud-related tickets
   - Check for repeat issues
   - Identify patterns

4. Combine all evidence:
   - Strong signals (any one = high confidence): DFRS critical, Behavioral flags, Fraud ring
   - Moderate signals: Historical patterns, medium DFRS scores
   - Weak signals: Single indicators

5. Make classification and determine confidence:
   - High confidence (>0.70) → Awaiting field Investigation
   - Medium (0.55-0.70) → Re-investigate
   - Low (<0.55) → Not fraud

6. Extract suspect information from case_details if mentioned

OUTPUT REQUIREMENTS:

Return ONLY valid JSON with this structure:

{INVESTIGATION_OUTPUT_FORMAT}

{INVESTIGATION_BUSINESS_RULES}

Analyze the case now:
"""
    
    return prompt


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def add_training_example(label: str, description: str, signals: str, result: str):
    """
    Add a new training example (for future learning system)
    
    Args:
        label: Short identifier (e.g., "NEW_PATTERN")
        description: Case description
        signals: Data signals observed
        result: Expected output
    """
    new_example = {
        "label": label,
        "description": description,
        "signals": signals,
        "result": result
    }
    
    TRAINING_EXAMPLES.append(new_example)
    print(f"✅ Added training example: {label}")
    print(f"   Total examples: {len(TRAINING_EXAMPLES)}")
    
    # TODO: Save to file for persistence
    return new_example


def get_prompt_version_info():
    """Get current prompt version information"""
    return {
        'version': PROMPT_VERSION,
        'last_updated': LAST_UPDATED,
        'training_examples_count': len(TRAINING_EXAMPLES),
        'changelog': CHANGELOG
    }
