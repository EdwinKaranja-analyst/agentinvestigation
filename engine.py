"""
M-KOPA Fraud Investigation Engine v2.2
Simple Freshdesk source ticket integration

New in v2.2:
- Auto-fetch Freshdesk source ticket from description
- Add as 'freshdesk_source' field for Claude analysis

Last Updated: 2025-11-18
"""

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
from queries_config import QUERIES, should_run_query, get_query_params

# ============================================================================
# ALLEGATION-SPECIFIC GUIDANCE
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
# DATABASE HELPERS
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
# API HELPERS
# ============================================================================

def fetch_freshservice_ticket(ticket_id):
    """Fetch ticket from Freshservice API"""
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


def fetch_freshdesk_ticket(ticket_id):
    """Fetch ticket from Freshdesk API"""
    api_key = os.getenv('FRESHDESK_API_KEY')
    if not api_key:
        print(f"   ⚠️  FRESHDESK_API_KEY not set, skipping Freshdesk fetch")
        return None
    
    try:
        # Fetch ticket
        url = f"https://m-kopa.freshdesk.com/api/v2/tickets/{ticket_id}"
        response = requests.get(url, auth=(api_key, 'X'), timeout=30)
        
        if response.status_code == 404:
            print(f"   ⚠️  Freshdesk ticket #{ticket_id} not found")
            return None
        
        response.raise_for_status()
        ticket = response.json()
        
        # Fetch conversations
        conv_url = f"https://m-kopa.freshdesk.com/api/v2/tickets/{ticket_id}/conversations"
        conv_response = requests.get(conv_url, auth=(api_key, 'X'), timeout=30)
        conversations = conv_response.json() if conv_response.status_code == 200 else []
        
        return {
            'ticket_id': ticket_id,
            'subject': ticket.get('subject', ''),
            'description': ticket.get('description_text', '') or ticket.get('description', ''),
            'case_details': ticket.get('custom_fields', {}).get('cf_case_details', ''),
            'conversations': conversations
        }
    
    except Exception as e:
        print(f"   ⚠️  Failed to fetch Freshdesk ticket: {e}")
        return None


def fetch_ticket(ticket_id):
    """
    Fetch ticket from Freshservice and auto-fetch Freshdesk source if referenced
    
    Args:
        ticket_id: Freshservice ticket ID
        
    Returns:
        Ticket data dict with optional 'freshdesk_source' field
    """
    # Fetch main Freshservice ticket
    ticket_data = fetch_freshservice_ticket(ticket_id)
    
    # Look for Freshdesk URL in description
    fd_pattern = r'https://m-kopa\.freshdesk\.com/helpdesk/tickets/(\d+)'
    fd_match = re.search(fd_pattern, ticket_data['description'])
    
    # If Freshdesk URL found, fetch that ticket too
    if fd_match:
        fd_ticket_id = fd_match.group(1)
        print(f"   🔗 Found Freshdesk source ticket #{fd_ticket_id}")
        
        fd_ticket = fetch_freshdesk_ticket(fd_ticket_id)
        if fd_ticket:
            ticket_data['freshdesk_source'] = fd_ticket
            print(f"   ✅ Fetched Freshdesk source ticket")
    
    return ticket_data


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
    
    return [dict(zip(columns, row)) for row in rows]


# ============================================================================
# CLAUDE HELPERS
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


def query_planning(ticket_data, historical_metadata=None):
    """
    Phase 1: Query Planning
    
    Args:
        ticket_data: Main ticket data
        historical_metadata: Optional list of historical ticket metadata
    """
    prompt_template = Path("prompts/query_planning.txt").read_text(encoding='utf-8')
    
    # Add historical metadata if available
    history_context = ""
    if historical_metadata:
        history_context = f"\n\nHISTORICAL TICKETS METADATA:\n{json.dumps(historical_metadata, indent=2)}\n"
        history_context += "\nBased on the historical tickets above, which ticket IDs are relevant to this investigation? "
        history_context += "Return them in 'relevant_historical_tickets' as a list of ticket IDs."
    
    prompt = prompt_template.format(
        ticket_data=json.dumps(ticket_data, indent=2),
        historical_context=history_context
    )
    
    return call_claude(prompt)


def investigate(ticket_data, account_data, query_results, query_plan):
    """
    Phase 2: Investigation
    
    Args:
        ticket_data: Main ticket data
        account_data: Account lookup results
        query_results: Dict of all query results (dfrs, login_risk, history, etc.)
        query_plan: Query planning response
    """
    prompt_template = Path("prompts/investigation.txt").read_text(encoding='utf-8')
    
    # Get allegation-specific guidance
    primary_allegation = query_plan.get('primary_allegation', '')
    allegation_key = primary_allegation.replace('_', '')
    guidance = ALLEGATION_GUIDANCE.get(allegation_key, "Standard investigation process")
    
    # Prepare ticket info
    subject = ticket_data.get('subject', '') if isinstance(ticket_data, dict) else ''
    description = ticket_data.get('description', '') if isinstance(ticket_data, dict) else ''
    
    # Format account data
    account_text = json.dumps(account_data, indent=2) if account_data else "Not found"
    
    # Format DFRS data
    dfrs_data = query_results.get('dfrs_signals')
    dfrs_text = "Not available"
    if dfrs_data:
        dfrs_text = f"""
Fraud Score: {dfrs_data.get('FraudScore', 0):.2f}
Tamper Score: {dfrs_data.get('HighestTamperScore', 0):.2f}
Zero Credit Days: {dfrs_data.get('ZeroCreditDaysConsecutive', 0)}
Tamper Reason: {dfrs_data.get('TamperReason', 'None')}
"""
    
    # Format login risk data
    login_risk_data = query_results.get('login_risk')
    login_risk_text = "Not available"
    if login_risk_data:
        login_risk_text = f"""
Device Risk Level: {login_risk_data.get('device_risk_level', 'Unknown')}
Unique Devices Used: {login_risk_data.get('unique_devices_used', 0)}
Max Customers Per Device: {login_risk_data.get('max_customers_per_device', 0)}
"""
    
    # Format payment match data
    payment_data = query_results.get('payment_match')
    payment_text = "Not available"
    if payment_data:
        payment_text = f"""
KYC Fraud Likelihood: {payment_data.get('kyc_fraud_likelihood', 'Unknown')}
Name Match Ratio: {payment_data.get('name_match_ratio', 0):.0%}
Phone Match Ratio: {payment_data.get('phone_match_ratio', 0):.0%}
"""
    
    # Format history
    history_data = query_results.get('historical_tickets', [])
    history_text = "No history"
    if history_data:
        if isinstance(history_data, list) and len(history_data) > 0:
            # Check if enhanced (has full ticket details)
            if isinstance(history_data[0], dict) and 'description' in history_data[0]:
                history_text = f"Found {len(history_data)} relevant tickets with full details:\n"
                for ticket in history_data:
                    history_text += f"\n--- Ticket #{ticket.get('ticket_id')} ---\n"
                    history_text += f"Subject: {ticket.get('subject')}\n"
                    history_text += f"Description: {ticket.get('description')[:200]}...\n"
            else:
                history_text = f"Found {len(history_data)} tickets"
    
    prompt = prompt_template.format(
        investigation_subject=query_plan.get('investigation_subject', 'unknown'),
        fraud_type=query_plan.get('fraud_type', 'unknown'),
        primary_allegation=primary_allegation,
        allegation_guidance=guidance,
        subject=subject,
        description=description,
        account_data=account_text,
        dfrs_data=dfrs_text,
        history_data=history_text,
        login_risk_data=login_risk_text,
        payment_data=payment_text
    )
    
    return call_claude(prompt)


# ============================================================================
# MAIN INVESTIGATION FUNCTION
# ============================================================================

def investigate_ticket(ticket_id, use_cache=True):
    """
    Main investigation function v2.3
    
    New in v2.3:
    - Query registry system for simple query management
    - Auto-execution based on allegation type
    - Smart historical ticket fetching (Claude selects relevant ones)
    
    Args:
        ticket_id: Freshservice ticket ID
        use_cache: Check cache first
        
    Returns:
        Investigation result dict
    """
    
    print(f"\n{'='*70}")
    print(f"🔍 INVESTIGATING TICKET #{ticket_id} (v2.3)")
    print(f"{'='*70}\n")
    
    # Check cache
    if use_cache:
        cached = get_investigation(ticket_id)
        if cached:
            print("✅ Found in cache")
            return cached
    
    result = {
        'ticket_id': ticket_id,
        'version': '2.3',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'phases': {}
    }
    
    try:
        # PHASE 1: Fetch ticket (with Freshdesk source if referenced)
        print("📥 Phase 1: Fetching ticket...")
        ticket_data = fetch_ticket(ticket_id)
        print(f"   ✅ Subject: {ticket_data['subject'][:60]}...")
        result['phases']['fetch'] = 'success'
        
        # PHASE 2: Fetch account data (always needed)
        print("\n📊 Phase 2: Fetching account data...")
        
        # First get basic query planning without history to extract identifiers
        preliminary_plan = query_planning(ticket_data)
        
        if preliminary_plan.get('wrong_escalation'):
            print("   ⚠️  WRONG ESCALATION DETECTED")
            print(f"   Reasoning: {preliminary_plan.get('reasoning')}")
            result['wrong_escalation'] = True
            result['query_plan'] = preliminary_plan
            result['success'] = True
            return result
        
        ids = preliminary_plan.get('identifiers', {})
        account_data = run_sql_query('account_lookup.sql', (
            ids.get('imei'),
            ids.get('phone'),
            ids.get('account_number')
        ))
        account = account_data[0] if account_data else None
        
        if account:
            print(f"   ✅ Account: {account.get('AccountNumber')}")
            print(f"   Device: {account.get('BrandModel')}")
        else:
            print("   ⚠️  No account found")
        
        result['phases']['account'] = account
        
        # PHASE 3: Get historical tickets metadata
        print("\n📊 Phase 3: Fetching historical tickets metadata...")
        historical_metadata = []
        if account:
            historical_metadata = run_sql_query('historical_tickets.sql', (
                account.get('IMEI'),
                account.get('AccountNumber')
            ))
            print(f"   Found {len(historical_metadata)} tickets")
        else:
            print("   ⏭️  Skipped (no account)")
        
        # PHASE 4: Query planning with historical metadata (Claude decides which to fetch)
        print("\n🤖 Phase 4: Query planning with historical context...")
        plan = query_planning(ticket_data, historical_metadata)
        
        print(f"   Investigation Subject: {plan.get('investigation_subject')}")
        print(f"   Fraud Type: {plan.get('fraud_type')}")
        print(f"   Allegation: {plan.get('primary_allegation')}")
        
        # Check which historical tickets Claude selected
        relevant_ticket_ids = plan.get('relevant_historical_tickets', [])
        if relevant_ticket_ids:
            print(f"   Claude selected {len(relevant_ticket_ids)} relevant historical tickets")
        
        result['phases']['planning'] = plan
        
        # PHASE 5: Fetch full details for relevant historical tickets
        enhanced_history = []
        if relevant_ticket_ids:
            print(f"\n📋 Phase 5: Fetching {len(relevant_ticket_ids)} relevant historical tickets...")
            for tid in relevant_ticket_ids:
                try:
                    hist_ticket = fetch_freshservice_ticket(str(tid))
                    enhanced_history.append(hist_ticket)
                    print(f"   ✅ Fetched #{tid}")
                except Exception as e:
                    print(f"   ⚠️  Failed to fetch #{tid}: {e}")
        else:
            print(f"\n⏭️  Phase 5: No relevant historical tickets selected")
        
        # PHASE 6: Execute queries based on allegation (registry-based)
        print(f"\n📊 Phase 6: Executing queries based on allegation...")
        primary_allegation = plan.get('primary_allegation', '')
        
        query_results = {}
        for query_name, config in QUERIES.items():
            # Skip historical_tickets (already handled)
            if query_name == 'historical_tickets':
                query_results[query_name] = enhanced_history
                continue
            
            # Check if query should run
            if should_run_query(query_name, primary_allegation, account):
                print(f"   Running {query_name}...")
                params = get_query_params(query_name, ids, account)
                
                try:
                    results = run_sql_query(config['file'], params)
                    query_results[query_name] = results[0] if results else None
                    
                    if results:
                        print(f"   ✅ {query_name} completed")
                    else:
                        print(f"   ⚠️  {query_name} returned no data")
                except Exception as e:
                    print(f"   ❌ {query_name} failed: {e}")
                    query_results[query_name] = None
            else:
                print(f"   ⏭️  Skipped {query_name}")
        
        result['phases']['queries'] = {k: 'completed' if v else 'no_data' for k, v in query_results.items()}
        
        # PHASE 7: Investigate with all data
        print("\n🔎 Phase 7: Analyzing...")
        investigation = investigate(ticket_data, account, query_results, plan)
        
        print(f"\n{'='*70}")
        print(f"✅ INVESTIGATION COMPLETE")
        print(f"{'='*70}")
        print(f"   Investigation Subject: {plan.get('investigation_subject')}")
        print(f"   Status: {investigation['fraud_status']}")
        print(f"   Confidence: {investigation['confidence']:.0%}")
        print(f"   Outcome: {investigation['case_outcome']}")
        
        # Show suspect if identified
        if investigation.get('suspect_type'):
            print(f"   Suspect Type: {investigation['suspect_type']}")
            if investigation.get('suspect_name'):
                print(f"   Suspect Name: {investigation['suspect_name']}")
        
        print(f"\n   Summary: {investigation['investigation_summary'][:100]}...")
        
        # Merge results
        result.update(investigation)
        result['success'] = True
        
        # Save to cache
        save_investigation(ticket_id, result)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        result['success'] = False
        result['error'] = str(e)
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
        print("Usage: python engine.py <ticket_id>")
        sys.exit(1)
    
    ticket_id = sys.argv[1]
    result = investigate_ticket(ticket_id)
    
    # Print result as JSON
    print(f"\n{'='*70}")
    print("FULL RESULT:")
    print(json.dumps(result, indent=2, default=str))