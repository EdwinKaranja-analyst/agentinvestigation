"""
M-KOPA Fraud Investigation Engine
Single file with all core logic
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


# =============================================================================
# DATABASE HELPERS
# =============================================================================

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


# =============================================================================
# API HELPERS
# =============================================================================

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
    sql = Path(f"sql\{query_file}").read_text()
    
    conn = get_azure_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return [dict(zip(columns, row)) for row in rows]


# =============================================================================
# CLAUDE HELPERS
# =============================================================================

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
    
    # Clean JSON from markdown and other formatting
    # Remove markdown code blocks
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    
    # Remove any text before the first {
    if '{' in text:
        text = text[text.find('{'):]
    
    # Remove any text after the last }
    if '}' in text:
        text = text[:text.rfind('}')+1]
    
    # Try to parse JSON
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        print(f"\n⚠️  JSON Parse Error: {e}")
        print(f"Raw response: {text[:200]}...")
        raise ValueError(f"Claude returned invalid JSON: {str(e)}")


def query_planning(ticket_data):
    """Phase 1: Decide what data to fetch"""
    prompt_template = Path(r"C:\Users\EdwinKaranja\fraud_investigation\rebuild\prompts\query_planning.txt").read_text()
    prompt = prompt_template.format(ticket_data=json.dumps(ticket_data, indent=2))
    
    return call_claude(prompt)


def investigate(ticket_data, account_data, dfrs_data, history_data):
    """Phase 2: Analyze and classify"""
    prompt_template = Path(r"C:\Users\EdwinKaranja\fraud_investigation\rebuild\prompts\investigation.txt").read_text()
    
    # Safely get ticket info
    subject = ticket_data.get('subject', '') if isinstance(ticket_data, dict) else ''
    details = ticket_data.get('case_details', '') if isinstance(ticket_data, dict) else ''
    
    prompt = prompt_template.format(
        subject=subject,
        details=details,
        account_data=json.dumps(account_data, indent=2) if account_data else "Not found",
        dfrs_data=json.dumps(dfrs_data, indent=2) if dfrs_data else "Not available",
        history_data=f"Found {len(history_data)} tickets" if history_data else "No history"
    )
    
    return call_claude(prompt)


# =============================================================================
# MAIN INVESTIGATION FUNCTION
# =============================================================================

def investigate_ticket(ticket_id, use_cache=True):
    """
    Main investigation function
    
    Args:
        ticket_id: Ticket to investigate
        use_cache: Check cache first
        
    Returns:
        Investigation result dict
    """
    
    print(f"\n{'='*70}")
    print(f"🔍 INVESTIGATING TICKET #{ticket_id}")
    print(f"{'='*70}\n")
    
    # Check cache
    if use_cache:
        cached = get_investigation(ticket_id)
        if cached:
            print("✅ Found in cache")
            return cached
    
    result = {
        'ticket_id': ticket_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'phases': {}
    }
    
    try:
        # PHASE 1: Fetch ticket
        print("📥 Phase 1: Fetching ticket...")
        ticket_data = fetch_ticket(ticket_id)
        print(f"   ✅ Subject: {ticket_data['subject'][:60]}...")
        result['phases']['fetch'] = 'success'
        
        # PHASE 2: Query planning
        print("\n🤖 Phase 2: Query planning...")
        plan = query_planning(ticket_data)
        print(f"   Fraud type: {plan['fraud_type']}")
        print(f"   Fetch DFRS: {'Yes' if plan['fetch_dfrs'] else 'No'}")
        print(f"   Fetch history: {'Yes' if plan['fetch_history'] else 'No'}")
        result['phases']['planning'] = plan
        
        # PHASE 3: Fetch account data (always)
        print("\n📊 Phase 3: Fetching account data...")
        ids = plan['identifiers']
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
        if plan['fetch_dfrs'] and account and account.get('SupportsDFRS'):
            print("\n📊 Phase 4: Fetching DFRS...")
            dfrs_results = run_sql_query('dfrs_signals.sql', (
                account.get('IMEI'),
                account.get('LoanID')
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
        if plan['fetch_history'] and account:
            print("\n📊 Phase 5: Fetching history...")
            history_data = run_sql_query('historical_tickets.sql', (
                account.get('IMEI'),
                account.get('AccountNumber')
            ))
            print(f"   Found {len(history_data)} tickets")
        else:
            print("\n⏭️  Phase 5: History skipped")
        
        result['phases']['history'] = history_data
        
        # PHASE 6: Investigate
        print("\n🔍 Phase 6: Analyzing...")
        investigation = investigate(ticket_data, account, dfrs_data, history_data)
        
        print(f"\n{'='*70}")
        print(f"✅ INVESTIGATION COMPLETE")
        print(f"{'='*70}")
        print(f"   Status: {investigation['fraud_status']}")
        print(f"   Confidence: {investigation['confidence']:.0%}")
        print(f"   Outcome: {investigation['case_outcome']}")
        print(f"\n   Summary: {investigation['summary']}")
        
        # Merge results
        result.update(investigation)
        result['success'] = True
        
        # Save to cache
        save_investigation(ticket_id, result)
        
        return result
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        result['success'] = False
        result['error'] = str(e)
        return result


# =============================================================================
# INITIALIZATION
# =============================================================================

# Initialize database on import
init_db()


# =============================================================================
# COMMAND LINE INTERFACE
# =============================================================================

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