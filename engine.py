"""
M-KOPA Fraud Investigation Engine v2.1
Evidence-based investigation with priority & case helper comments

Updates in v2.1:
- Auto-update Freshservice with priority and case comments
- Wrong escalation flagging with special handling
- Priority calculation (1=Low, 2=Medium, 3=High, 4=Critical)
- Case helper comments for fraud team

Updates in v2.0:
- Investigation subject identification (Customer/DSR/External)
- Allegation-specific decision guidance
- Wrong escalation detection
- Evidence-based thresholds from 632 cases

Last Updated: 2025-11-26
"""

# TODO:Consider an option to pick out data from database instead of Freshservice
# TODO: Avoid updating tickets in groups like Arrest tracker.


import os
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
import anthropic
import pyodbc
import struct
from azure.identity import AzureCliCredential

import config

# ============================================================================
# ALLEGATION-SPECIFIC GUIDANCE (NEW IN V2.0)
# ============================================================================

ALLEGATION_GUIDANCE = {
    'resale': """
PRIMARY CHECK: Are payments continuing?
- YES → NOT fraud (legitimate transfer/gift - ALLOWED BY POLICY)
- NO → Likely fraud (payment breach)
Note: Resale allowed if customer/recipient pays. No field investigation for resale.
Fraud Rate: 70% | Key: Payment status determines fraud, not resale itself
""",
    
    'identity_theft': """
PRIMARY CHECK: DSR involved?
- YES → CRITICAL PATTERN: DSR stolen ID fraud
  → Classification: Confirmed/Likely fraud
  → Outcome: DSR investigation + discipline
- NO → Check family member application
  → Family member + Payments continuing → NOT fraud (cultural norm in Kenya)
  → Customer confirms account → NOT fraud
  → Customer denies + KYC issues → Investigate
Fraud Rate: 25.2% | Note: 74% NOT fraud (often family applications)
""",
    
    'cash_loan_fraud': """
IDENTIFY SCENARIO (3 patterns):
1. External Scam (60%): Facebook, OTP fishing, call from fraudster
   → Suspect: External | Outcome: Field investigation
2. Account Takeover (30%): PIN reset, password change, hijacking
   → Suspect: External | Outcome: Field investigation  
3. Customer Denies (10%): Loan appeared, customer claims no knowledge
   → Investigate thoroughly
Fraud Rate: 58.8% (HIGHEST!) | Priority: CRITICAL
""",
    
    'hacking_tampering': """
QUICK TRIAGE (<5 min for 79% of cases):
- Generic device complaint ("not working", "screen", "battery")
  + DFRS TamperScore < 0.6
  → NOT fraud (92% confidence) → Auto-close, refer to tech support
- Specific tampering ("lock disabled", "IMEI changed", "bypass")
  + DFRS TamperScore > 0.9
  → Likely fraud (88% confidence) → Field investigation needed
Fraud Rate: 21.2% | Note: 79% NOT fraud (device malfunctions)
Classification: Usually "Likely fraud" until field verification confirms
""",
    
    'hardware_theft': """
PRIMARY CHECK: Device active after theft date?
- YES → NOT fraud (false alarm, customer still has device)
- NO → Check DSR involvement
  → DSR identified → Likely fraud (DSR discipline)
  → Police report filed + Device inactive → Legitimate theft → No action
Fraud Rate: 30% | Note: 70% NOT fraud (false alarms common)
""",
    
    '3rd_party_cash': """
PRIMARY CHECK: Customer knowledge of payer?
- Customer DENIES knowing payer → Likely fraud (90% confidence)
  → Pattern: Extortion/coercion → Field investigation
- Customer CONFIRMS arrangement → NOT fraud
  → Pattern: Voluntary family assistance → No action
Fraud Rate: 68.2% | Suspect ID Rate: 68.2% (high)
""",
    
    'cash_payments': """
CHECK: Payment source analysis
- Mpesa recipient ≠ Customer name + Customer unreachable → Likely fraud
- DSR involved + Unauthorized cash collection → Confirmed fraud (DSR discipline)
- New account + Immediate cash loan → Likely fraud
Fraud Rate: 54.4% | Often overlaps with cash_loan_fraud
""",
    
    'mis_selling': """
CHECK: DSR pattern?
- Repeated complaints vs same DSR → Confirmed fraud (DSR discipline)
- First complaint + Serious discrepancy → Likely fraud (DSR warning)
- Minor misunderstanding + Customer satisfied → NOT fraud
Fraud Rate: 45.5% | Note: 82% end in "No action" (resolved with explanation)
"""
}

# ============================================================================
# DATABASE HELPERS (unchanged from original)
# ============================================================================

def init_db():
    """Initialize SQLite cache"""
    conn = sqlite3.connect(config.CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS investigations (
            ticket_id TEXT PRIMARY KEY,
            date TIMESTAMP,
            fraud_status TEXT,
            confidence REAL,
            data JSON
        )
    """)
    conn.commit()
    conn.close()


def save_investigation(ticket_id, result):
    """Save investigation to cache"""
    conn = sqlite3.connect(config.CACHE_DB)
    conn.execute("""
        INSERT OR REPLACE INTO investigations VALUES (?, ?, ?, ?, ?)
    """, (
        ticket_id,
        datetime.now(timezone.utc).isoformat(),
        result.get('fraud_status'),
        result.get('confidence'),
        json.dumps(result, default=str)
    ))
    conn.commit()
    conn.close()


def get_investigation(ticket_id):
    """Get cached investigation"""
    conn = sqlite3.connect(config.CACHE_DB)
    row = conn.execute(
        "SELECT data FROM investigations WHERE ticket_id = ?",
        (ticket_id,)
    ).fetchone()
    conn.close()
    return json.loads(row[0]) if row else None


# ============================================================================
# API HELPERS (unchanged from original)
# ============================================================================

def fetch_ticket(ticket_id):
    """Fetch ticket from Freshservice"""
    api_key = os.getenv('FRESHSERVICE_API_KEY')
    if not api_key:
        raise ValueError("FRESHSERVICE_API_KEY not set")
    
    url = f"{config.FRESHSERVICE_URL}/tickets/{ticket_id}?include=conversations"
    response = requests.get(url, auth=(api_key, 'X'), timeout=30)
    response.raise_for_status()
    
    data = response.json()
    ticket = data.get('ticket', data)
    
    return {
        'ticket_id': ticket_id,
        'subject': ticket.get('subject', ''),
        'description': ticket.get('description_text', '') or ticket.get('description', ''),
        'case_details': ticket.get('custom_fields', {}).get('case_details', ''),
        'conversations': data.get('conversations', [])
    }


def get_azure_connection():
    """Connect to Azure Synapse"""
    credential = AzureCliCredential()
    token = credential.get_token('https://database.windows.net/')
    
    token_bytes = bytes(token[0], "UTF-8")
    encoded_token = b''.join(bytes({b}) + bytes(1) for b in token_bytes)
    token_struct = struct.pack("=i", len(encoded_token)) + encoded_token
    
    conn_string = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={config.SYNAPSE_SERVER};"
        f"DATABASE={config.SYNAPSE_DATABASE};"
    )
    
    return pyodbc.connect(conn_string, attrs_before={1256: token_struct})


def run_sql_query(query_file, params):
    """Run SQL query from file"""
    sql = Path(f"sql/{query_file}").read_text(encoding='utf-8')
    
    conn = get_azure_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Handle encoding issues in results
    results = []
    for row in rows:
        row_dict = {}
        for col, val in zip(columns, row):
            if isinstance(val, str):
                # Clean up any encoding issues
                try:
                    val = val.encode('latin1').decode('utf-8')
                except (UnicodeDecodeError, UnicodeEncodeError):
                    # If conversion fails, keep as-is but replace problematic chars
                    val = val.encode('utf-8', errors='replace').decode('utf-8')
            row_dict[col] = val
        results.append(row_dict)
    
    return results


# ============================================================================
# PRIORITY & COMMENT HELPERS (NEW IN V2.1)
# ============================================================================

def calculate_priority(investigation, plan, dfrs_data):
    """
    Calculate ticket priority (1=Low, 2=Medium, 3=High, 4=Critical)
    Based on fraud confidence, allegation type, and evidence strength
    
    Args:
        investigation: Investigation results
        plan: Query plan with allegation info
        dfrs_data: DFRS signals if available
        
    Returns:
        int: Priority score (1-4)
    """
    
    fraud_status = investigation.get('fraud_status', '')
    confidence = investigation.get('confidence', 0)
    allegation = plan.get('primary_allegation', '')
    suspect_identified = bool(investigation.get('suspect_name'))
    
    # CRITICAL (4)
    if allegation == 'cash_loan_fraud':
        return 4  # 58.8% fraud rate - always critical
    
    if suspect_identified:
        return 4  # 99.3% fraud accuracy when suspect identified
    
    if dfrs_data:
        tamper = dfrs_data.get('TamperScore', 0)
        zero_days = dfrs_data.get('ZeroCreditDaysConsecutive', 0)
        if tamper > 0.9 and zero_days > 30:
            return 4  # 88% fraud confidence
    
    if allegation == '3rd_party_cash' and confidence > 0.85:
        return 4  # Customer denies knowing payer = 90% fraud
    
    # HIGH (3)
    if fraud_status == 'Confirmed fraud':
        return 3
    
    if allegation == 'resale' and confidence > 0.65:
        return 3  # 70% fraud rate
    
    if allegation == 'hardware_theft' and plan.get('investigation_subject') == 'dsr':
        return 3  # DSR theft
    
    if allegation == 'identity_theft' and plan.get('allegation_specific_checks', {}).get('identity_dsr_involved'):
        return 3  # DSR stolen ID
    
    if dfrs_data and dfrs_data.get('FraudScore', 0) > 0.7:
        return 3
    
    # MEDIUM (2)
    if fraud_status == 'Likely fraud' and confidence >= 0.55:
        return 2
    
    if allegation in ['mis_selling', 'cash_payments']:
        return 2
    
    # LOW (1)
    if fraud_status == 'Not fraud':
        return 1
    
    if allegation == 'hacking_tampering':
        tampering_type = plan.get('allegation_specific_checks', {}).get('tampering_description_type')
        if tampering_type == 'generic_device_issue':
            return 1  # 79% NOT fraud
    
    # Default MEDIUM
    return 2


def generate_case_helper_comment(investigation, plan, account_data, dfrs_data, wrong_escalation=False):
    """
    Generate internal case helper comment for fraud team
    
    Args:
        investigation: Investigation results
        plan: Query plan
        account_data: Account info
        dfrs_data: DFRS signals
        wrong_escalation: Is this a wrong escalation?
        
    Returns:
        str: Formatted comment for Freshservice
    """
    
    lines = []
    lines.append("🤖 AI INVESTIGATION SUMMARY")
    lines.append("=" * 50)
    
    # Wrong escalation handling
    if wrong_escalation:
        lines.append("⚠️ WRONG ESCALATION DETECTED")
        lines.append("")
        lines.append(f"Reason: {plan.get('reasoning', 'Not a fraud case')}")
        lines.append("")
        lines.append("RECOMMENDED ACTION:")
        lines.append("• Reassign to appropriate team")
        lines.append("• Update ticket category")
        lines.append("• Close if insufficient information")
        return "\n".join(lines)
    
    # Priority and confidence
    fraud_status = investigation.get('fraud_status', 'Unknown')
    confidence = investigation.get('confidence', 0)
    lines.append(f"Status: {fraud_status} ({confidence:.0%} confidence)")
    
    # Investigation subject
    subject = plan.get('investigation_subject', 'unknown')
    lines.append(f"Investigation Subject: {subject.title()}")
    
    # Allegation
    allegation = plan.get('primary_allegation', 'unknown')
    lines.append(f"Allegation: {allegation.replace('_', ' ').title()}")
    
    # Suspect info if identified
    if investigation.get('suspect_name'):
        lines.append("")
        lines.append("⚠️ SUSPECT IDENTIFIED (99.3% fraud accuracy)")
        lines.append(f"Type: {investigation.get('suspect_type', 'Unknown')}")
        lines.append(f"Name: {investigation.get('suspect_name')}")
        if investigation.get('suspect_number'):
            lines.append(f"Phone: {investigation.get('suspect_number')}")
    
    # Account info
    if account_data:
        lines.append("")
        lines.append("📊 ACCOUNT INFO:")
        lines.append(f"Account: {account_data.get('AccountNumber', 'N/A')}")
        lines.append(f"Device: {account_data.get('BrandModel', 'N/A')}")
        if account_data.get('IMEI'):
            lines.append(f"IMEI: {account_data.get('IMEI')}")
    
    # DFRS signals if available
    if dfrs_data:
        lines.append("")
        lines.append("📊 DFRS SIGNALS:")
        lines.append(f"Fraud Score: {dfrs_data.get('FraudScore', 0):.2f}")
        lines.append(f"Tamper Score: {dfrs_data.get('TamperScore', 0):.2f}")
        lines.append(f"Zero Credit Days: {dfrs_data.get('ZeroCreditDaysConsecutive', 0)}")
        if dfrs_data.get('TamperReason'):
            lines.append(f"Tamper Reason: {dfrs_data.get('TamperReason')}")
    
    # Key evidence
    evidence = investigation.get('key_evidence', [])
    if evidence:
        lines.append("")
        lines.append("🔍 KEY EVIDENCE:")
        for item in evidence[:5]:  # Top 5
            lines.append(f"• {item}")
    
    # Recommended outcome
    outcome = investigation.get('case_outcome', 'Unknown')
    lines.append("")
    lines.append("✅ RECOMMENDED OUTCOME:")
    lines.append(f"{outcome}")
    
    # Investigation summary
    lines.append("")
    lines.append("📝 ANALYSIS:")
    lines.append(investigation.get('investigation_summary', 'No summary available'))
    
    # Add timestamp
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"System Version: 2.1")
    
    return "\n".join(lines)


def update_freshservice_ticket(ticket_id, priority, comment):
    """
    Update Freshservice ticket with priority and internal note
    
    Args:
        ticket_id: Ticket ID
        priority: 1-4 (1=Low, 2=Medium, 3=High, 4=Critical)
        comment: Case helper comment (internal note)
        
    Returns:
        bool: True if successful
    """
    api_key = os.getenv('FRESHSERVICE_API_KEY')
    if not api_key:
        raise ValueError("FRESHSERVICE_API_KEY not set")
    
    # Update priority
    url = f"{config.FRESHSERVICE_URL}/tickets/{ticket_id}"
    update_data = {
        "priority": priority
    }
    
    response = requests.put(
        url, 
        json=update_data,
        auth=(api_key, 'X'),
        timeout=30
    )
    response.raise_for_status()
    
    # Add internal note
    notes_url = f"{config.FRESHSERVICE_URL}/tickets/{ticket_id}/notes"
    note_data = {
        "body": comment,
        "private": True  # Internal note - not visible to customer
    }
    
    response = requests.post(
        notes_url,
        json=note_data,
        auth=(api_key, 'X'),
        timeout=30
    )
    response.raise_for_status()
    
    return True


# ============================================================================
# CLAUDE HELPERS (UPDATED FOR V2.0)
# ============================================================================

def call_claude(prompt):
    """Call Claude API"""
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    
    client = anthropic.Anthropic(api_key=api_key)
    
    response = client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=config.MAX_TOKENS,
        temperature=config.TEMPERATURE,
        messages=[{"role": "user", "content": prompt}]
    )
    
    text = response.content[0].text.strip()
    
    # Clean JSON from markdown
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    
    if '{' in text:
        text = text[text.find('{'):]
    if '}' in text:
        text = text[:text.rfind('}')+1]
    
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        print(f"\n⚠️  JSON Parse Error: {e}")
        print(f"Raw response: {text[:200]}...")
        raise ValueError(f"Claude returned invalid JSON: {str(e)}")


def query_planning(ticket_data):
    """
    Phase 1: Query Planning (v2.0)
    
    New in v2.0:
    - Wrong escalation detection
    - Investigation subject identification
    - Allegation-specific checks
    """
    prompt_template = Path("prompts/query_planning.txt").read_text(encoding='utf-8')
    prompt = prompt_template.format(ticket_data=json.dumps(ticket_data, indent=2, default=str))
    
    return call_claude(prompt)


def investigate(ticket_data, account_data, dfrs_data, history_data, query_plan):
    """
    Phase 2: Investigation (v2.0)
    
    New in v2.0:
    - Investigation subject context
    - Allegation-specific guidance
    - Evidence-based thresholds
    """
    try:
        prompt_template = Path("prompts/investigation.txt").read_text(encoding='utf-8')
        
        # Get allegation-specific guidance
        primary_allegation = query_plan.get('primary_allegation', '')
        allegation_key = primary_allegation.replace('_', '')  # Remove underscores for key matching
        guidance = ALLEGATION_GUIDANCE.get(allegation_key, "Standard investigation process")
        
        # Prepare ticket info - handle various field names
        subject = ''
        details = ''
        
        if isinstance(ticket_data, dict):
            subject = ticket_data.get('subject', '')
            # Try multiple possible field names for details
            details = (
                ticket_data.get('case_details') or 
                ticket_data.get('description') or 
                ticket_data.get('description_text') or 
                ''
            )
        
        # Format account data
        account_text = json.dumps(account_data, indent=2, default=str) if account_data else "Not found"
        
        # Format DFRS data
        dfrs_text = "Not available"
        if dfrs_data:
            dfrs_text = f"""
Fraud Score: {dfrs_data.get('FraudScore', 0):.2f}
Tamper Score: {dfrs_data.get('TamperScore', 0) or dfrs_data.get('HighestTamperScore', 0):.2f}
Zero Credit Days: {dfrs_data.get('ZeroCreditDaysConsecutive', 0)}
Tamper Reason: {dfrs_data.get('TamperReason', 'None')}
"""
        
        # Format history
        history_text = f"Found {len(history_data)} tickets" if history_data else "No history"
        
        # Prepare all possible template variables (for backward compatibility with v2.0 prompts)
        template_vars = {
            'investigation_subject': query_plan.get('investigation_subject', 'unknown'),
            'fraud_type': query_plan.get('fraud_type', 'unknown'),
            'primary_allegation': primary_allegation,
            'allegation_guidance': guidance,
            'subject': subject,
            'details': details,
            'description': details,  # Backward compatibility
            'account_data': account_text,
            'dfrs_data': dfrs_text,
            'history_data': history_text,
            'login_risk_data': "Not available",  # For v2.0 compatibility
            'payment_match_data': "Not available",  # For v2.0 compatibility
            'payment_data': "Not available"  # For v2.0 compatibility
        }
        
        # Safe formatting that handles ANY missing keys
        import re
        
        def safe_format(template, variables):
            """Format template with variables, providing 'Not available' for missing keys"""
            def replace_var(match):
                key = match.group(1)
                return str(variables.get(key, "Not available"))
            
            return re.sub(r'\{(\w+)\}', replace_var, template)
        
        prompt = safe_format(prompt_template, template_vars)
        
        return call_claude(prompt)
        
    except Exception as e:
        print(f"   Error in investigate(): {str(e)}")
        print(f"   ticket_data type: {type(ticket_data)}")
        if isinstance(ticket_data, dict):
            print(f"   ticket_data keys: {list(ticket_data.keys())}")
        raise


# ============================================================================
# MAIN INVESTIGATION FUNCTION (UPDATED FOR V2.1)
# ============================================================================

def investigate_ticket(ticket_id, use_cache=True, update_freshservice=True):
    """
    Main investigation function v2.1
    
    New in v2.1:
    - Auto-update Freshservice with priority and comments
    - Wrong escalation flagging
    
    New in v2.0:
    - Wrong escalation detection
    - Investigation subject identification
    - Allegation-specific decision logic
    
    Args:
        ticket_id: Ticket to investigate
        use_cache: Check cache first
        update_freshservice: Update Freshservice ticket (default True)
        
    Returns:
        Investigation result dict
    """
    
    print(f"\n{'='*70}")
    print(f"🔍 INVESTIGATING TICKET #{ticket_id} (v2.1)")
    print(f"{'='*70}\n")
    
    # Check cache
    if use_cache:
        cached = get_investigation(ticket_id)
        if cached:
            print("✅ Found in cache")
            return cached
    
    result = {
        'ticket_id': ticket_id,
        'version': '2.1',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'phases': {}
    }
    
    try:
        # PHASE 1: Fetch ticket
        print("📥 Phase 1: Fetching ticket...")
        ticket_data = fetch_ticket(ticket_id)
        print(f"   ✅ Subject: {ticket_data['subject'][:60]}...")
        result['phases']['fetch'] = 'success'
        
        # PHASE 2: Query planning (v2.0 - includes wrong escalation check)
        print("\n🤖 Phase 2: Query planning...")
        plan = query_planning(ticket_data)
        
        # Check for wrong escalation (NEW IN V2.0)
        if plan.get('wrong_escalation'):
            print("   ⚠️  WRONG ESCALATION DETECTED")
            print(f"   Reasoning: {plan.get('reasoning')}")
            
            result['wrong_escalation'] = True
            result['query_plan'] = plan
            result['success'] = True
            
            # For wrong escalations, set low priority and special comment
            result['priority'] = 1
            result['priority_label'] = 'Low'
            result['case_helper_comment'] = generate_case_helper_comment(
                {}, plan, None, None, wrong_escalation=True
            )
            
            # Update Freshservice for wrong escalations too
            if update_freshservice:
                try:
                    update_freshservice_ticket(
                        ticket_id,
                        result['priority'],
                        result['case_helper_comment']
                    )
                    print("   ✅ Freshservice updated (wrong escalation flagged)")
                    result['freshservice_updated'] = True
                except Exception as e:
                    print(f"   ⚠️  Freshservice update failed: {e}")
                    result['freshservice_updated'] = False
            
            # Save to cache
            save_investigation(ticket_id, result)
            
            return result
        
        print(f"   Investigation Subject: {plan.get('investigation_subject')}")
        print(f"   Fraud Type: {plan.get('fraud_type')}")
        print(f"   Allegation: {plan.get('primary_allegation')}")
        print(f"   Fetch DFRS: {'Yes' if plan.get('execute_dfrs') else 'No'}")
        print(f"   Fetch history: {'Yes' if plan.get('execute_history') else 'No'}")
        result['phases']['planning'] = plan
        
        # PHASE 3: Fetch account data (always)
        print("\n📊 Phase 3: Fetching data...")
        ids = plan.get('identifiers', {})
        account_data = run_sql_query('account_lookup.sql', (
            ids.get('imei'),
            ids.get('loan_id'),
            ids.get('account_number')
        ))
        account = account_data[0] if account_data else None
        
        if account:
            print(f"   ✅ Account: {account.get('AccountNumber')}")
            print(f"   Device: {account.get('BrandModel')}")
        else:
            print("   ⚠️  No account found")
        
        result['phases']['account'] = account
        
        # PHASE 4: Fetch DFRS (conditional)
        dfrs_data = None
        if plan.get('execute_dfrs') and account and account.get('SupportsDFRS'):
            print("\n📊 Phase 4: Fetching DFRS...")
            dfrs_results = run_sql_query('dfrs_signals.sql', (
                account.get('IMEI'),
                account.get('AccountNumber')
            ))
            dfrs_data = dfrs_results[0] if dfrs_results else None
            
            if dfrs_data:
                print(f"   Fraud Score: {dfrs_data.get('FraudScore', 0):.2f}")
                print(f"   Tamper Score: {dfrs_data.get('HighestTamperScore', 0):.2f}")
        else:
            print("\n⏭️  Phase 4: DFRS skipped")
        
        result['phases']['dfrs'] = dfrs_data
        
        # PHASE 5: Fetch history (conditional)
        history_data = []
        if plan.get('execute_history') and account:
            print("\n📊 Phase 5: Fetching history...")
            history_data = run_sql_query('historical_tickets.sql', (
                account.get('IMEI'),
                account.get('AccountNumber')
            ))
            print(f"   Found {len(history_data)} tickets")
        else:
            print("\n⏭️  Phase 5: History skipped")
        
        result['phases']['history'] = history_data
        
        # PHASE 6: Investigate (v2.0 - with allegation-specific guidance)
        print("\n🔍 Phase 6: Analyzing...")
        investigation = investigate(ticket_data, account, dfrs_data, history_data, plan)
        
        # PHASE 7: Calculate priority & generate comment (NEW IN V2.1)
        print("\n📊 Phase 7: Generating priority & comment...")
        priority = calculate_priority(investigation, plan, dfrs_data)
        priority_label = {1: 'Low', 2: 'Medium', 3: 'High', 4: 'Critical'}[priority]
        case_comment = generate_case_helper_comment(investigation, plan, account, dfrs_data)
        
        investigation['priority'] = priority
        investigation['priority_label'] = priority_label
        investigation['case_helper_comment'] = case_comment
        
        print(f"\n{'='*70}")
        print(f"✅ INVESTIGATION COMPLETE")
        print(f"{'='*70}")
        print(f"   Investigation Subject: {plan.get('investigation_subject')}")
        print(f"   Status: {investigation['fraud_status']}")
        print(f"   Confidence: {investigation['confidence']:.0%}")
        print(f"   Priority: {priority_label} ({priority})")
        print(f"   Outcome: {investigation['case_outcome']}")
        
        # Show suspect if identified (NEW IN V2.0)
        if investigation.get('suspect_type'):
            print(f"   Suspect Type: {investigation['suspect_type']}")
            if investigation.get('suspect_name'):
                print(f"   Suspect Name: {investigation['suspect_name']}")
        
        print(f"\n   Summary: {investigation['investigation_summary'][:100]}...")
        
        # PHASE 8: Update Freshservice (NEW IN V2.1)
        if update_freshservice:
            print("\n📤 Phase 8: Updating Freshservice...")
            try:
                update_freshservice_ticket(ticket_id, priority, case_comment)
                print("   ✅ Freshservice updated")
                investigation['freshservice_updated'] = True
            except Exception as e:
                print(f"   ⚠️  Freshservice update failed: {e}")
                investigation['freshservice_updated'] = False
        else:
            print("\n⏭️  Phase 8: Freshservice update skipped")
            investigation['freshservice_updated'] = False
        
        # Merge results
        result.update(investigation)
        result['success'] = True
        
        # Save to cache
        save_investigation(ticket_id, result)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        print(f"\nFull traceback:")
        traceback.print_exc()
        result['success'] = False
        result['error'] = str(e)
        result['error_traceback'] = traceback.format_exc()
        return result


# ============================================================================
# INITIALIZATION
# ============================================================================

# Initialize database on import
init_db()


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python engine.py <ticket_id> [--no-update]")
        print("  --no-update: Skip Freshservice update")
        sys.exit(1)
    
    ticket_id = sys.argv[1]
    update_fs = '--no-update' not in sys.argv
    
    result = investigate_ticket(ticket_id, update_freshservice=update_fs)
    
    # Print result as JSON
    print(f"\n{'='*70}")
    print("FULL RESULT:")
    print(json.dumps(result, indent=2, default=str))