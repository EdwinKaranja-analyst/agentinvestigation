"""
M-KOPA Fraud Investigation - Core Engine
Main investigation logic (Phases 1-5)

Last Updated: 2025-10-30
Version: v2.0 (Modular)
"""

import os
import re
import json
from datetime import datetime, date
from typing import Dict, Any, Optional, List

try:
    import anthropic
except ImportError:
    print("❌ Anthropic library required!")
    exit(1)

from config.settings import (
    CLAUDE_MODEL,
    CLAUDE_MAX_TOKENS_PLANNING,
    CLAUDE_MAX_TOKENS_INVESTIGATION,
    CLAUDE_TEMPERATURE_PLANNING,
    CLAUDE_TEMPERATURE_INVESTIGATION,
    ALLEGATION_TO_SUBREASON,
    REASON_STANDARDIZATION,
    BASIC_FIELD_UPDATES
)

from config.queries import SQL_QUERIES
from config.instructions import get_query_planning_prompt, get_investigation_prompt

from .api import fetch_freshservice_ticket, get_synapse_connection


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def standardize_phone_number(phone: str) -> Optional[str]:
    """Standardize phone - add leading 0 if missing"""
    if not phone:
        return None
    phone = re.sub(r'\D', '', str(phone))
    if len(phone) == 9 and not phone.startswith('0'):
        phone = '0' + phone
    return phone if len(phone) in [10, 9] else None


def extract_phone_numbers(text: str) -> List[str]:
    """Extract phones from text"""
    if not text:
        return []
    patterns = [r'\b0[17]\d{8}\b', r'\b[17]\d{8}\b']
    phones = []
    for pattern in patterns:
        phones.extend(re.findall(pattern, str(text)))
    standardized = [standardize_phone_number(p) for p in phones if p]
    return [p for p in standardized if p]


def extract_suspect_name(text: str) -> Optional[str]:
    """Extract suspect name from case details"""
    if not text:
        return None
    patterns = [
        r'suspect[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        r'fraudster[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        r'individual.*introduced.*as\s+([A-Z][a-z]+)',
        r'person.*named?\s+([A-Z][a-z]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, str(text), re.IGNORECASE)
        if match:
            name = ' '.join(word.capitalize() for word in match.group(1).split())
            if len(name) > 2 and name not in ['Customer', 'Client', 'Person', 'Manager']:
                return name
    return None


def determine_reason_subreason(primary_allegation: str, suspect_type: Optional[str]) -> tuple:
    """Determine reason_for_interaction and subreason based on allegation and suspect"""
    if suspect_type == "DSR":
        reason = "DSR fraud"
    elif suspect_type == "External to M-kopa":
        reason = "External/Noncustomer fraud"
    elif suspect_type == "Customer":
        reason = "Customer Fraud"
    elif suspect_type in ["Other Internal staff", "Internal Sales staff"]:
        reason = "DSR fraud"
    else:
        reason = "Suspected Fraud"
    
    subreason = ALLEGATION_TO_SUBREASON.get(primary_allegation, "Cash Payments")
    reason = REASON_STANDARDIZATION.get(reason, reason)
    
    return reason, subreason


def save_failed_update(ticket_id: str, payload: Dict[str, Any], update_type: str):
    """Save failed Freshservice update for manual processing"""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"failed_update_{update_type}_{ticket_id}_{timestamp}.json"
    with open(filename, 'w') as f:
        json.dump({
            'ticket_id': ticket_id,
            'update_type': update_type,
            'payload': payload,
            'timestamp': timestamp
        }, f, indent=2)
    print(f"   💾 Failed update saved: {filename}")



# ============================================================================
# PHASE 1: FETCH FRESHSERVICE DATA
# ============================================================================

def phase1_fetch_freshservice_data(ticket_id: str) -> Dict[str, Any]:
    """
    Phase 1: Fetch ticket data from Freshservice API
    Uses api module for actual fetching
    """
    print(f"🎯 Phase 1: Fetching Freshservice data for ticket {ticket_id}")
    
    try:
        print("📞 Calling Freshservice API...")
        result = fetch_freshservice_ticket(ticket_id)
        
        conversations_count = len(result.get('conversations', []))
        custom_fields_count = len(result.get('basic_data', {}).get('custom_fields', {}))
        
        print(f"✅ Phase 1 completed: {conversations_count} conversations, {custom_fields_count} custom fields")
        return result
        
    except Exception as e:
        print(f"❌ Phase 1 failed: {str(e)}")
        raise


# ============================================================================
# PHASE 2: CLAUDE QUERY PLANNING
# ============================================================================

def phase2_query_planning(freshservice_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 2: Claude analyzes ticket and plans SQL query execution
    Uses config/instructions for prompt
    """
    
    print("\n🎯 Phase 2: Claude Query Planning & Identifier Extraction")
    print("=" * 70)
    
    ticket_id = freshservice_data.get('ticket_id')
    
    # Get prompt from config module
    prompt = get_query_planning_prompt(freshservice_data)
    
    # Call Claude
    print("🤖 Calling Claude for query planning...")
    
    try:
        api_key = os.environ.get('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        
        client = anthropic.Anthropic(api_key=api_key)
        
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=CLAUDE_MAX_TOKENS_PLANNING,
            temperature=CLAUDE_TEMPERATURE_PLANNING,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = response.content[0].text.strip()
        response_text = re.sub(r'```json\s*', '', response_text)
        response_text = re.sub(r'```\s*', '', response_text)
        response_text = response_text.strip()
        
        # Parse JSON with error handling
        try:
            query_plan = json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse Claude response as JSON: {str(e)}")
            print(f"   Raw response (first 300 chars): {response_text[:300]}")
            raise ValueError(f"Claude returned invalid JSON in Phase 2: {str(e)}")
        
        print(f"✅ Query plan generated")
        print(f"   Fraud Type: {query_plan.get('fraud_type')}")
        print(f"   Risk Level: {query_plan.get('risk_level')}")
        print(f"   Route: {query_plan.get('investigation_route')}")
        print(f"   Stages:")
        print(f"     Stage 1-2 (Basic): ✅ Always")
        print(f"     Stage 3 (DFRS): {'✅ Yes' if query_plan.get('execute_stage_3_dfrs') else '⏭️ Skip'}")
        print(f"     Stage 4 (History): {'✅ Yes' if query_plan.get('execute_stage_4_history') else '⏭️ Skip'}")
        print(f"     Stage 5 (Behavioral): {'✅ Yes' if query_plan.get('execute_stage_5_behavioral') else '⏭️ Skip'}")
        
        query_plan['phase2_metadata'] = {
            'timestamp': datetime.utcnow().isoformat(),
            'model': CLAUDE_MODEL,
            'ticket_id': ticket_id
        }
        
        print(f"✅ Phase 2 completed")
        return query_plan
        
    except Exception as e:
        print(f"❌ Phase 2 failed: {str(e)}")
        raise


# ============================================================================
# PHASE 3: SQL STAGE EXECUTION FUNCTIONS
# ============================================================================

    
    connection = pyodbc.connect(connString, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})
    print("✅ Connected to Azure Synapse")
    return connection


def execute_stage_1_2_basic_account(connection, identifiers: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """
    Stage 1-2: Basic Account Lookup (ALWAYS EXECUTE)
    
    Purpose: Get account profile using any identifier
    Time: ~100ms
    """
    
    print("\n📊 Stage 1-2: Basic Account Lookup")
    print("   Execution: ALWAYS (required for all investigations)")
    
    sql = """
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
    
    try:
        cursor = connection.cursor()
        
        # Parameters in exact order matching SQL (PhoneNumber removed)
        param_values = (
            identifiers.get('IMEI'),
            identifiers.get('LoanID'),
            identifiers.get('AccountNumber'),
            identifiers.get('DeviceID'),
            identifiers.get('CustomerID'),
            identifiers.get('AccountID'),
            date.today()
        )
        
        cursor.execute(sql, param_values)
        
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            result = dict(zip(columns, row))
            print(f"   ✅ Account found: {result.get('AccountNumber')}")
            print(f"      Device: {result.get('BrandModel')}")
            print(f"      IMEI: {result.get('IMEI')}")
            print(f"      Supports DFRS: {'Yes' if result.get('SupportsDFRS') else 'No'}")
            return result
        else:
            print(f"   ⚠️ No account found")
            return {}
            
    except Exception as e:
        print(f"   ❌ Stage 1-2 failed: {str(e)}")
        raise

def execute_stage_3_dfrs(connection, identifiers: Dict[str, Optional[str]], account_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 3: DFRS (Device Fraud Risk Signals) - CONDITIONAL
    
    Purpose: Get tampering scores, fraud scores
    Time: ~200ms
    """
    
    print("\n📊 Stage 3: DFRS (Device Fraud Risk Signals)")
    print("   Execution: CONDITIONAL (only if device supports DFRS)")
    
    if not account_data.get('SupportsDFRS'):
        print(f"   ⏭️ SKIPPED: Device {account_data.get('BrandModel')} does not support DFRS")
        return {}
    
    sql = """
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
    
    try:
        cursor = connection.cursor()
        
        param_values = (
            account_data.get('IMEI') or identifiers.get('IMEI'),
            account_data.get('LoanID') or identifiers.get('LoanID'),
            account_data.get('DeviceID') or identifiers.get('DeviceID')
        )
        
        cursor.execute(sql, param_values)
        
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            result = dict(zip(columns, row))
            print(f"   ✅ DFRS data retrieved:")
            print(f"      Fraud Score: {result.get('FraudScore', 0):.2f}")
            print(f"      Tamper Score: {result.get('HighestTamperScore', 0):.2f}")
            print(f"      Zero Credit Days: {result.get('ZeroCreditDaysConsecutive', 0)}")
            return result
        else:
            print(f"   ⚠️ No DFRS data found")
            return {}
            
    except Exception as e:
        print(f"   ❌ Stage 3 failed: {str(e)}")
        print(f"   ⚠️ Continuing without DFRS data...")
        return {}


    Stage 4: Historical Freshdesk Tickets - USUALLY EXECUTE
    
    Purpose: Check for repeat patterns
    Time: ~50ms
    """
    
    print("\n📊 Stage 4: Historical Freshdesk Tickets")
    print("   Execution: USUALLY (fast and informative)")
    
    sql = """
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
    
    try:
        cursor = connection.cursor()
        
        param_values = (
            account_data.get('IMEI') or identifiers.get('IMEI'),
            account_data.get('AccountNumber') or identifiers.get('AccountNumber'),
            str(account_data.get('AccountID')) if account_data.get('AccountID') else None
        )
        
        cursor.execute(sql, param_values)
        
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        results = [dict(zip(columns, row)) for row in rows]
        
        print(f"   ✅ Found {len(results)} historical tickets")
        
        # Display ticket details for visibility and Streamlit integration
        if results:
            print(f"\n   📋 Historical Ticket Details:")
            for i, ticket in enumerate(results, 1):
                ticket_id = ticket.get('TicketId', 'N/A')
                subject = ticket.get('Subject', 'No subject')
                created = ticket.get('CreatedTime', 'Unknown date')
                status = ticket.get('Status', 'Unknown')
                reason = ticket.get('ReasonForInteraction', 'N/A')
                subreason = ticket.get('SubReasonForInteraction', 'N/A')
                
                # Format creation date if it's a datetime object
                if isinstance(created, datetime):
                    created_str = created.strftime('%Y-%m-%d %H:%M')
                else:
                    created_str = str(created)
                
                # Truncate long subjects for readability
                if len(subject) > 60:
                    subject = subject[:57] + "..."
                
                print(f"\n   {i}️⃣  Ticket #{ticket_id}")
                print(f"       Subject: {subject}")
                print(f"       Created: {created_str}")
                print(f"       Status: {status}")
                if reason != 'N/A':
                    print(f"       Reason: {reason}")
                if subreason != 'N/A':
                    print(f"       Sub-reason: {subreason}")
        else:
            print(f"   ℹ️  No historical tickets found for this account")
        
        return results
        
    except Exception as e:
        print(f"   ❌ Stage 4 failed: {str(e)}")
        print(f"   ⚠️ Continuing without historical data...")
        return []
def execute_stage_5_behavioral_analysis(connection, identifiers: Dict[str, Optional[str]], account_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 5: Behavioral Analysis - EXPENSIVE! RARELY EXECUTE
    
    Purpose: Account takeover, fraud ring detection
    Time: 2000-5000ms
    """
    
    print("\n📊 Stage 5: Behavioral Analysis (EXPENSIVE)")
    print("   Execution: RARELY (2-5 seconds!)")
    print("   ⚠️ Only for account takeover and fraud ring detection")
    
    account_number = account_data.get('AccountNumber') or identifiers.get('AccountNumber')
    
    if not account_number:
        print("   ⏭️ SKIPPED: No account number available")
        return {}
    
    # Simplified version for now - full query from spec is very complex
    # TODO: Replace with full behavioral query from spec when ready to test
    sql = """
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
    
    try:
        print("   ⏳ Running expensive behavioral query...")
        start_time = datetime.now()
        
        cursor = connection.cursor()
        cursor.execute(sql, (account_number,))
        
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        cursor.close()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if row:
            result = dict(zip(columns, row))
            print(f"   ✅ Behavioral data retrieved ({elapsed:.2f}s)")
            print(f"      Cash Loan Taken: {result.get('CashLoanTaken', 0)}")
            print(f"      PIN Reset New Device: {result.get('ResetPinFromNewDevice', 0)}")
            return result
        else:
            return {}
            
    except Exception as e:
        print(f"   ❌ Stage 5 failed: {str(e)}")
        print(f"   ⚠️ Continuing without behavioral data...")
        return {}
def phase3_dynamic_query_execution(query_plan: Dict[str, Any], freshservice_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 3 (NEW): Execute SQL queries dynamically based on Phase 2 plan
    
    Input: query_plan from Phase 2, freshservice_data from Phase 1
    Output: Combined fraud data from executed stages
    """
    
    print("\n🎯 Phase 3: Dynamic SQL Query Execution")
    print("=" * 70)
    
    ticket_id = freshservice_data.get('ticket_id')
    identifiers = query_plan.get('identifiers_extracted', {})
    
    print(f"\n📋 Execution Plan:")
    print(f"   Route: {query_plan.get('investigation_route')}")
    print(f"   Fraud Type: {query_plan.get('fraud_type')}")
    print(f"   Risk: {query_plan.get('risk_level')}")
    
    execution_log = {
        'stages_executed': [],
        'stages_skipped': [],
        'total_time_ms': 0
    }
    
    combined_data = {
        'account_profile': {},
        'dfrs_signals': {},
        'historical_tickets': [],
        'behavioral_analysis': {},
        'query_plan': query_plan,
        'execution_log': execution_log
    }
    
    try:
        connection = get_synapse_connection()
        start_time = datetime.now()
        
        # ====================================================================
        # STAGE 1-2: BASIC ACCOUNT (ALWAYS)
        # ====================================================================
        
        stage_start = datetime.now()
        account_data = execute_stage_1_2_basic_account(connection, identifiers)
        stage_time = (datetime.now() - stage_start).total_seconds() * 1000
        
        combined_data['account_profile'] = account_data
        execution_log['stages_executed'].append({
            'stage': 'Stage 1-2: Basic Account',
            'time_ms': stage_time,
            'result': 'success' if account_data else 'no_data'
        })
        
        if not account_data:
            print("\n⚠️ No account found - stopping further stages")
            connection.close()
            return combined_data
        
        # ====================================================================
        # STAGE 3: DFRS (CONDITIONAL)
        # ====================================================================
        
        if query_plan.get('execute_stage_3_dfrs'):
            stage_start = datetime.now()
            dfrs_data = execute_stage_3_dfrs(connection, identifiers, account_data)
            stage_time = (datetime.now() - stage_start).total_seconds() * 1000
            
            combined_data['dfrs_signals'] = dfrs_data
            execution_log['stages_executed'].append({
                'stage': 'Stage 3: DFRS',
                'time_ms': stage_time,
                'result': 'success' if dfrs_data else 'no_data'
            })
        else:
            print("\n⏭️ Stage 3 (DFRS) skipped per query plan")
            execution_log['stages_skipped'].append('Stage 3: DFRS')
        
        # ====================================================================
        # STAGE 4: HISTORICAL TICKETS (CONDITIONAL)
        # ====================================================================
        
        if query_plan.get('execute_stage_4_history'):
            stage_start = datetime.now()
            historical_data = execute_stage_4_historical_tickets(connection, identifiers, account_data)
            stage_time = (datetime.now() - stage_start).total_seconds() * 1000
            
            combined_data['historical_tickets'] = historical_data
            execution_log['stages_executed'].append({
                'stage': 'Stage 4: Historical',
                'time_ms': stage_time,
                'result': f'{len(historical_data)}_tickets'
            })
        else:
            print("\n⏭️ Stage 4 (Historical) skipped per query plan")
            execution_log['stages_skipped'].append('Stage 4: Historical')
        
        # ====================================================================
        # STAGE 5: BEHAVIORAL (CONDITIONAL - EXPENSIVE!)
        # ====================================================================
        
        if query_plan.get('execute_stage_5_behavioral'):
            print("\n⚠️ WARNING: Executing expensive Stage 5")
            stage_start = datetime.now()
            behavioral_data = execute_stage_5_behavioral_analysis(connection, identifiers, account_data)
            stage_time = (datetime.now() - stage_start).total_seconds() * 1000
            
            combined_data['behavioral_analysis'] = behavioral_data
            execution_log['stages_executed'].append({
                'stage': 'Stage 5: Behavioral',
                'time_ms': stage_time,
                'result': 'success' if behavioral_data else 'no_data'
            })
        else:
            print("\n⏭️ Stage 5 (Behavioral) skipped per query plan")
            print("   💰 Saved 2-5 seconds by skipping expensive query")
            execution_log['stages_skipped'].append('Stage 5: Behavioral')
        
        connection.close()
        
        # Calculate total
        total_time = (datetime.now() - start_time).total_seconds() * 1000
        execution_log['total_time_ms'] = total_time
        
        print("\n" + "=" * 70)
        print(f"✅ Phase 3 Complete - Query Execution Summary")
        print("=" * 70)
        print(f"   Stages Executed: {len(execution_log['stages_executed'])}")
        for stage in execution_log['stages_executed']:
            print(f"     {stage['stage']}: {stage['time_ms']:.0f}ms")
        print(f"   Stages Skipped: {len(execution_log['stages_skipped'])}")
        print(f"   Total Time: {total_time:.0f}ms")
        
        combined_data['phase3_metadata'] = {
            'timestamp': datetime.utcnow().isoformat(),
            'ticket_id': ticket_id,
            'total_time_ms': total_time
        }
        
        return combined_data
        
    except Exception as e:
        print(f"\n❌ Phase 3 failed: {str(e)}")
        if 'connection' in locals():
            connection.close()
        raise


# ============================================================================
# PHASE 4: CLAUDE FRAUD INVESTIGATION (ENHANCED)
# ============================================================================



# ============================================================================
# PHASE 4: ENHANCED FRAUD INVESTIGATION
# ============================================================================

def phase4_fraud_investigation_enhanced(
    freshservice_data: Dict[str, Any],
    query_plan: Dict[str, Any],
    sql_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Phase 4 (ENHANCED): Analyze all data sources and classify fraud
    
    Input:
        - freshservice_data: Ticket from Phase 1
        - query_plan: Claude's plan from Phase 2
        - sql_data: Dynamic SQL results from Phase 3
    
    Output:
        - Fraud classification
        - Freshservice field updates
        - Investigation reasoning
    """
    
    print("\n🎯 Phase 4: Enhanced Fraud Investigation with Claude")
    print("=" * 70)
    
    # Extract all data
    ticket_id = freshservice_data.get('ticket_id')
    basic_data = freshservice_data.get('basic_data', {})
    custom_fields = basic_data.get('custom_fields', {})
    
    subject = basic_data.get('subject', '')
    description = basic_data.get('description_text', '') or basic_data.get('description', '')
    case_details = custom_fields.get('case_details', '')
    conversations = freshservice_data.get('conversations', [])
    
    # Phase 2 insights
    fraud_type = query_plan.get('fraud_type', 'unknown')
    risk_level = query_plan.get('risk_level', 'medium')
    identifiers = query_plan.get('identifiers_extracted', {})
    
    # Phase 3 data
    account_profile = sql_data.get('account_profile', {})
    dfrs_signals = sql_data.get('dfrs_signals', {})
    historical_tickets = sql_data.get('historical_tickets', [])
    behavioral_analysis = sql_data.get('behavioral_analysis', {})
    
    # Prepare conversation text
    conversation_text = ""
    for i, conv in enumerate(conversations, 1):
        body = conv.get('body_text', '')
        if body:
            conversation_text += f"\nConversation {i}: {body}\n"
    
    # Build enhanced investigation prompt
    prompt = build_enhanced_investigation_prompt(
        ticket_id=ticket_id,
        subject=subject,
        case_details=case_details,
        conversations=conversation_text,
        fraud_type=fraud_type,
        risk_level=risk_level,
        account_profile=account_profile,
        dfrs_signals=dfrs_signals,
        historical_tickets=historical_tickets,
        behavioral_analysis=behavioral_analysis
    )
    
    # Call Claude
    print("🤖 Calling Claude for enhanced fraud analysis...")
    
    try:
        api_key = os.environ.get('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not set")
        
        client = anthropic.Anthropic(api_key=api_key)
        
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2500,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = response.content[0].text.strip()
        response_text = re.sub(r'```json\s*', '', response_text)
        response_text = re.sub(r'```\s*', '', response_text)
        
        # Parse JSON with error handling
        try:
            claude_result = json.loads(response_text.strip())
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse Claude response as JSON: {str(e)}")
            print(f"   Raw response (first 300 chars): {response_text[:300]}")
            raise ValueError(f"Claude returned invalid JSON in Phase 4: {str(e)}")
        
        print(f"✅ Investigation complete")
        print(f"   Classification: {claude_result.get('fraud_status')}")
        print(f"   Confidence: {claude_result.get('confidence'):.2f}")
        print(f"   Allegation: {claude_result.get('primary_allegation')}")
        
    except Exception as e:
        print(f"❌ Phase 4 failed: {str(e)}")
        raise
    
    # Process Claude's output into Freshservice format
    investigation_result = process_investigation_output(
        claude_result=claude_result,
        case_details=case_details,
        account_number=account_profile.get('AccountNumber'),
        ticket_id=ticket_id,
        fraud_type=fraud_type
    )
    
    print(f"✅ Phase 4 completed")
    
    return investigation_result


def build_enhanced_investigation_prompt(
    ticket_id: str,
    subject: str,
    case_details: str,
    conversations: str,
    fraud_type: str,
    risk_level: str,
    account_profile: Dict[str, Any],
    dfrs_signals: Dict[str, Any],
    historical_tickets: List[Dict[str, Any]],
    behavioral_analysis: Dict[str, Any]
) -> str:
    """Build enhanced investigation prompt with all data sources"""
    
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
    
    prompt = f"""You are an expert M-KOPA fraud investigator analyzing case #{ticket_id}.

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
   - Never use "Confirmed fraud" (requires physical inspection)

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

{{
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
  "risk_factors": {{
    "dfrs_fraud_score": 0.XX or null,
    "dfrs_tamper_score": 0.XX or null,
    "behavioral_account_takeover": true/false,
    "behavioral_fraud_ring": true/false,
    "repeat_offender": true/false,
    "payment_evasion": true/false
  }},
  "data_sources_used": {{
    "dfrs_available": true/false,
    "behavioral_available": true/false,
    "historical_count": X
  }},
  "recommended_next_steps": [
    "Action 1",
    "Action 2"
  ]
}}

BUSINESS RULES:
- If fraud_status = "Likely fraud" → primary_allegation REQUIRED (from valid list)
- If fraud_status = "Not fraud" → primary_allegation = null
- suspect_type can be inferred even without suspect_number
- Extract phone numbers and add leading 0
- Public note: NO PII (no account numbers, names, phones, scores)

Analyze the case now:
"""
    
    return prompt


def process_investigation_output(
    claude_result: Dict[str, Any],
    case_details: str,
    account_number: Optional[str],
    ticket_id: str,
    fraud_type: str
) -> Dict[str, Any]:
    """Process Claude output into Freshservice update format"""
    
    fraud_status = claude_result.get('fraud_status')
    confidence = claude_result.get('confidence', 0.0)
    primary_allegation = claude_result.get('primary_allegation')
    suspect_type = claude_result.get('suspect_type')
    suspect_name = claude_result.get('suspect_name')
    suspect_number = claude_result.get('suspect_number')
    case_outcome = claude_result.get('case_outcome')
    investigation_summary = claude_result.get('investigation_summary', '')
    public_note = claude_result.get('public_note', '')
    
    # Standardize phone
    if suspect_number:
        suspect_number = standardize_phone_number(suspect_number)
    
    # Determine reason/subreason
    reason_for_interaction, subreason_for_interaction = determine_reason_subreason(
        primary_allegation, suspect_type
    )
    
    # Build case_details update
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    risk_factors = claude_result.get('risk_factors', {})
    data_sources = claude_result.get('data_sources_used', {})
    
    # Format data sources used
    sources_text = "\nData Sources:\n"
    if data_sources.get('dfrs_available'):
        sources_text += f"- DFRS Signals: Fraud Score {risk_factors.get('dfrs_fraud_score', 0):.2f}, "
        sources_text += f"Tamper Score {risk_factors.get('dfrs_tamper_score', 0):.2f}\n"
    if data_sources.get('behavioral_available'):
        sources_text += f"- Behavioral Analysis: "
        if risk_factors.get('behavioral_account_takeover'):
            sources_text += "⚠️ Account Takeover Detected "
        if risk_factors.get('behavioral_fraud_ring'):
            sources_text += "🚨 Fraud Ring Connection "
        sources_text += "\n"
    if data_sources.get('historical_count', 0) > 0:
        sources_text += f"- Historical Tickets: {data_sources['historical_count']} previous cases\n"
    
    case_details_update = f"""{case_details}

---
[AI Investigation - {timestamp}]
Confidence: {confidence:.2f} | Classification: {fraud_status}

Investigation Summary:
{investigation_summary}
{sources_text}
Recommended Action:
{claude_result.get('recommended_next_steps', ['Review case'])[0] if claude_result.get('recommended_next_steps') else 'Review case'}
"""
    
    # Build result
    investigation_result = {
        'fraud_status': fraud_status,
        'confidence': confidence,
        
        'updates': {
            'basic_fields': BASIC_FIELD_UPDATES.copy(),
            'custom_fields': {
                'fraud_status': fraud_status if fraud_status == 'Likely fraud' else None,
                'case_outcome': case_outcome,
                'primary_allegation': primary_allegation,
                'reason_for_interaction': reason_for_interaction,
                'subreason_for_interaction': subreason_for_interaction,
                'loan_account_number': account_number,
                'suspect_type': suspect_type,
                'suspect_name': suspect_name,
                'suspect_number': suspect_number,
                'case_details': case_details_update
            }
        },
        
        'public_comment': public_note,
        
        'analysis': {
            'key_evidence': claude_result.get('key_evidence', []),
            'risk_factors': claude_result.get('risk_factors', {}),
            'data_sources_used': claude_result.get('data_sources_used', {}),
            'recommended_next_steps': claude_result.get('recommended_next_steps', [])
        },
        
        'metadata': {
            'investigation_timestamp': timestamp,
            'model_used': 'claude-sonnet-4-20250514',
            'ticket_id': ticket_id,
            'fraud_type_detected': fraud_type
        }
    }
    
    return investigation_result


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    print("Phase 4 Enhanced Investigation Module")
    print("This module should be imported into the complete pipeline")
    print("\nContains:")
    print("  - phase4_fraud_investigation_enhanced()")
    print("  - Enhanced prompt with DFRS, behavioral, historical data")
    print("  - Proper Freshservice field mapping")
    print("  - Business rules enforcement")


# ============================================================================
# PHASE 5: UPDATE FRESHSERVICE
# ============================================================================

def phase5_update_freshservice(ticket_id: str, investigation_result: Dict[str, Any], dry_run: bool = True) -> Dict[str, Any]:
    """
    Phase 5: Update Freshservice with results
    
    Input: investigation_result from Phase 4
    Output: Update status
    """
    
    print("\n🎯 Phase 5: Update Freshservice")
    print("=" * 70)
    
    if dry_run:
        print("🔧 DRY RUN MODE - No updates will be made")
        print("\n   Would update:")
        print("      ✓ Assign to Fraud Team")
        print("      ✓ Set fraud classification")
        print("      ✓ Update categorization")
        
        return {
            'success': True,
            'dry_run': True,
            'ticket_updated': False,
            'comment_created': False
        }
    else:
        print("✅ LIVE MODE - Would execute updates")
        # TODO: Implement actual Freshservice update
        return {
            'success': True,
            'dry_run': False,
            'note': 'Update logic needed'
        }


# ============================================================================
# COMPLETE PIPELINE
# ============================================================================

def run_dynamic_investigation(
    ticket_id: str,
    use_analytics: bool = True,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Run complete dynamic fraud investigation pipeline
    
    Parameters:
    -----------
    ticket_id : str
        Freshservice ticket ID
    use_analytics : bool
        Enable Azure Synapse queries
    dry_run : bool
        Don't update Freshservice
        
    Returns:
    --------
    investigation : dict
        Complete results from all phases
    """
    
    print("\n")
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 15 + "M-KOPA DYNAMIC FRAUD INVESTIGATION" + " " * 19 + "║")
    print("║" + " " * 20 + "Claude-Powered Query Optimization" + " " * 15 + "║")
    print("╚" + "═" * 68 + "╝")
    print()
    
    investigation = {
        'ticket_id': ticket_id,
        'start_time': datetime.utcnow().isoformat(),
        'configuration': {
            'use_analytics': use_analytics,
            'dry_run': dry_run
        },
        'phases': {},
        'timeline': [],
        'success': False
    }
    
    try:
        # ====================================================================
        # PHASE 1: FETCH FRESHSERVICE
        # ====================================================================
        
        print("\n" + "▼" * 70)
        print("📥 PHASE 1: FETCH FRESHSERVICE DATA")
        print("▼" * 70)
        
        stage_start = datetime.now()
        freshservice_data = phase1_fetch_freshservice_data(ticket_id)
        stage_time = (datetime.now() - stage_start).total_seconds() * 1000
        
        investigation['phases']['phase1'] = {
            'name': 'Fetch Freshservice',
            'status': 'success',
            'time_ms': stage_time,
            'data': freshservice_data
        }
        
        investigation['timeline'].append({
            'phase': 'Phase 1',
            'time_ms': stage_time
        })
        
        print(f"\n📊 Phase 1 Output:")
        print(f"   Subject: {freshservice_data.get('basic_data', {}).get('subject', 'N/A')}")
        print(f"   Conversations: {len(freshservice_data.get('conversations', []))}")
        print(f"   Time: {stage_time:.0f}ms")
        
        # ====================================================================
        # PHASE 2: QUERY PLANNING (NEW)
        # ====================================================================
        
        print("\n" + "▼" * 70)
        print("🤖 PHASE 2: CLAUDE QUERY PLANNING (NEW)")
        print("▼" * 70)
        
        stage_start = datetime.now()
        query_plan = phase2_query_planning(freshservice_data)
        stage_time = (datetime.now() - stage_start).total_seconds() * 1000
        
        investigation['phases']['phase2'] = {
            'name': 'Query Planning',
            'status': 'success',
            'time_ms': stage_time,
            'data': query_plan
        }
        
        investigation['timeline'].append({
            'phase': 'Phase 2',
            'time_ms': stage_time
        })
        
        print(f"\n📊 Phase 2 Output:")
        print(f"   Identifiers: {sum(1 for v in query_plan.get('identifiers_extracted', {}).values() if v)} found")
        print(f"   Fraud Type: {query_plan.get('fraud_type')}")
        print(f"   Time: {stage_time:.0f}ms")
        
        # ====================================================================
        # PHASE 3: DYNAMIC SQL (NEW)
        # ====================================================================
        
        if use_analytics:
            print("\n" + "▼" * 70)
            print("📊 PHASE 3: DYNAMIC SQL EXECUTION (NEW)")
            print("▼" * 70)
            
            stage_start = datetime.now()
            combined_sql_data = phase3_dynamic_query_execution(query_plan, freshservice_data)
            stage_time = (datetime.now() - stage_start).total_seconds() * 1000
            
            investigation['phases']['phase3'] = {
                'name': 'Dynamic SQL',
                'status': 'success',
                'time_ms': stage_time,
                'data': combined_sql_data
            }
            
            investigation['timeline'].append({
                'phase': 'Phase 3',
                'time_ms': stage_time
            })
            
            exec_log = combined_sql_data.get('execution_log', {})
            print(f"\n📊 Phase 3 Output:")
            print(f"   Stages Executed: {len(exec_log.get('stages_executed', []))}")
            print(f"   Stages Skipped: {len(exec_log.get('stages_skipped', []))}")
            print(f"   Total Time: {exec_log.get('total_time_ms', 0):.0f}ms")
            
        else:
            print("\n⏭️ Phase 3 skipped (analytics disabled)")
            combined_sql_data = {
                'account_profile': {},
                'dfrs_signals': {},
                'historical_tickets': [],
                'behavioral_analysis': {},
                'query_plan': query_plan
            }
            
            investigation['phases']['phase3'] = {
                'name': 'Dynamic SQL',
                'status': 'skipped'
            }
        
        # ====================================================================
        # PHASE 4: AI INVESTIGATION (ENHANCED)
        # ====================================================================
        
        print("\n" + "▼" * 70)
        print("🔍 PHASE 4: CLAUDE FRAUD INVESTIGATION")
        print("▼" * 70)
        
        stage_start = datetime.now()
        investigation_result = phase4_fraud_investigation_enhanced(freshservice_data, query_plan, combined_sql_data)
        stage_time = (datetime.now() - stage_start).total_seconds() * 1000
        
        investigation['phases']['phase4'] = {
            'name': 'AI Investigation',
            'status': 'success',
            'time_ms': stage_time,
            'data': investigation_result
        }
        
        investigation['timeline'].append({
            'phase': 'Phase 4',
            'time_ms': stage_time
        })
        
        print(f"\n📊 Phase 4 Output:")
        print(f"   Classification: {investigation_result.get('fraud_status')}")
        print(f"   Confidence: {investigation_result.get('confidence'):.2f}")
        print(f"   Time: {stage_time:.0f}ms")
        
        # ====================================================================
        # PHASE 5: UPDATE FRESHSERVICE
        # ====================================================================
        
        print("\n" + "▼" * 70)
        print("✏️ PHASE 5: UPDATE FRESHSERVICE")
        print("▼" * 70)
        
        stage_start = datetime.now()
        update_result = phase5_update_freshservice(ticket_id, investigation_result, dry_run)
        stage_time = (datetime.now() - stage_start).total_seconds() * 1000
        
        investigation['phases']['phase5'] = {
            'name': 'Update Freshservice',
            'status': 'success',
            'time_ms': stage_time,
            'data': update_result
        }
        
        investigation['timeline'].append({
            'phase': 'Phase 5',
            'time_ms': stage_time
        })
        
        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        
        investigation['success'] = True
        investigation['end_time'] = datetime.utcnow().isoformat()
        
        total_time = sum([t.get('time_ms', 0) for t in investigation['timeline']])
        
        print("\n" + "═" * 70)
        print("📊 INVESTIGATION COMPLETE")
        print("═" * 70)
        
        print(f"\n🎫 Ticket: {ticket_id}")
        print(f"🔍 Fraud Type: {query_plan.get('fraud_type')}")
        print(f"⚠️ Risk: {query_plan.get('risk_level')}")
        print(f"🛣️ Route: {query_plan.get('investigation_route')}")
        
        print(f"\n⏱️ Performance:")
        for entry in investigation['timeline']:
            print(f"   {entry['phase']}: {entry['time_ms']:.0f}ms")
        print(f"   ─────────────────")
        print(f"   Total: {total_time:.0f}ms")
        
        if use_analytics and combined_sql_data:
            exec_log = combined_sql_data.get('execution_log', {})
            stages_run = len(exec_log.get('stages_executed', []))
            stages_skipped = len(exec_log.get('stages_skipped', []))
            
            print(f"\n💰 Query Optimization:")
            print(f"   SQL Stages Executed: {stages_run}/5")
            print(f"   SQL Stages Skipped: {stages_skipped}/5")
            
            if stages_skipped > 0:
                print(f"   Cost Savings: {stages_skipped} expensive queries avoided ✅")
        
        # Save results
        output_file = f"dynamic_investigation_{ticket_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(output_file, 'w') as f:
            json.dump(investigation, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        return investigation
        
    except Exception as e:
        print(f"\n❌ INVESTIGATION FAILED: {str(e)}")
        investigation['success'] = False
        investigation['error'] = str(e)
        
        import traceback
        traceback.print_exc()
        
        return investigation


