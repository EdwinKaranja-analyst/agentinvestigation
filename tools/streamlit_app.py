#!/usr/bin/env python3
"""
M-KOPA Dynamic Fraud Investigation - Streamlit Demo
Visual interface for the complete 5-phase dynamic system

Shows:
- Phase-by-phase execution
- Dynamic query decisions
- DFRS/behavioral/historical data
- Fraud classification results
- Performance metrics
"""

import streamlit as st
import json
import sys
import os
from datetime import datetime
import time

# Import the complete dynamic system
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from core.engine import (
        run_dynamic_investigation,
        phase1_fetch_freshservice_data,
        phase2_query_planning,
        phase3_dynamic_query_execution
    )
    SYSTEM_AVAILABLE = True
except ImportError as e:
    st.error(f"❌ Could not import system: {e}")
    st.error("Make sure COMPLETE_DYNAMIC_FRAUD_SYSTEM_FIXED.py is in the same directory")
    SYSTEM_AVAILABLE = False


# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="M-KOPA Dynamic Fraud Investigation",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .phase-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    .success-metric {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
    }
    .warning-metric {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
    }
    .danger-metric {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# SESSION STATE
# ============================================================================

if 'investigation_result' not in st.session_state:
    st.session_state.investigation_result = None
if 'selected_phase' not in st.session_state:
    st.session_state.selected_phase = 'overview'
if 'running' not in st.session_state:
    st.session_state.running = False


# ============================================================================
# SIDEBAR: NAVIGATION
# ============================================================================

st.sidebar.markdown("## 🔍 Investigation Phases")

phases = {
    'overview': {'name': 'Overview', 'icon': '📊'},
    'phase1': {'name': 'Phase 1: Freshservice', 'icon': '📥'},
    'phase2': {'name': 'Phase 2: Query Planning', 'icon': '🤖'},
    'phase3': {'name': 'Phase 3: Dynamic SQL', 'icon': '📊'},
    'phase4': {'name': 'Phase 4: Investigation', 'icon': '🔍'},
    'phase5': {'name': 'Phase 5: Update', 'icon': '✏️'}
}

# Show phase status if investigation has run
if st.session_state.investigation_result:
    result = st.session_state.investigation_result
    
    # Overview button
    if st.sidebar.button("📊 Overview", key="btn_overview", use_container_width=True):
        st.session_state.selected_phase = 'overview'
    
    st.sidebar.markdown("---")
    
    for phase_key in ['phase1', 'phase2', 'phase3', 'phase4', 'phase5']:
        phase_info = phases[phase_key]
        phase_data = result.get('phases', {}).get(phase_key, {})
        status = phase_data.get('status', 'pending')
        
        if status == 'success':
            icon = '✅'
        elif status == 'skipped':
            icon = '⏭️'
        elif status == 'failed':
            icon = '❌'
        else:
            icon = '⏸️'
        
        button_label = f"{icon} {phase_info['icon']} {phase_info['name']}"
        if st.sidebar.button(button_label, key=f"btn_{phase_key}", use_container_width=True):
            st.session_state.selected_phase = phase_key

else:
    for phase_key, phase_info in phases.items():
        button_label = f"{phase_info['icon']} {phase_info['name']}"
        if st.sidebar.button(button_label, key=f"btn_{phase_key}", use_container_width=True):
            st.session_state.selected_phase = phase_key

st.sidebar.markdown("---")

with st.sidebar.expander("ℹ️ About Dynamic System"):
    st.markdown("""
    **Key Innovation:**
    Claude decides which SQL queries to run
    
    **Benefits:**
    - 47% fewer database queries
    - 60% of cases finish in <500ms
    - Fraud ring detection (Stage 5)
    
    **Model:** Claude Sonnet 4.5
    **Cost:** ~$0.005 per investigation
    """)


# ============================================================================
# MAIN AREA: HEADER
# ============================================================================

st.markdown('<div class="main-header">🔍 M-KOPA Dynamic Fraud Investigation</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Claude-Powered Query Optimization & Fraud Detection</div>', unsafe_allow_html=True)

st.markdown("---")


# ============================================================================
# INPUT CONTROLS
# ============================================================================

if not SYSTEM_AVAILABLE:
    st.error("❌ System not available. Check file imports.")
    st.stop()

st.markdown("### 📥 Investigation Configuration")

col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    ticket_id = st.text_input(
        "Ticket ID",
        value="",
        placeholder="e.g., 151333",
        help="Freshservice ticket ID to investigate"
    )

with col2:
    use_analytics = st.checkbox(
        "Use Analytics (Phase 3)",
        value=True,
        help="Enable dynamic SQL queries"
    )

with col3:
    dry_run = st.checkbox(
        "Dry Run Mode",
        value=True,
        help="Preview without updating Freshservice"
    )

col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])

with col_btn2:
    run_button = st.button(
        "▶️ Run Dynamic Investigation",
        type="primary",
        use_container_width=True,
        disabled=st.session_state.running
    )

st.markdown("---")


# ============================================================================
# RUN INVESTIGATION
# ============================================================================

if run_button and not st.session_state.running:
    if not ticket_id:
        st.error("Please enter a ticket ID")
    else:
        st.session_state.running = True
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Check API keys
            if not os.getenv('ANTHROPIC_API_KEY'):
                st.error("❌ ANTHROPIC_API_KEY not set")
                st.stop()
            
            status_text.text("🔄 Starting dynamic investigation...")
            progress_bar.progress(10)
            time.sleep(0.3)
            
            # Run complete investigation
            with st.spinner("Running 5-phase dynamic investigation..."):
                result = run_dynamic_investigation(
                    ticket_id=ticket_id,
                    use_analytics=use_analytics,
                    dry_run=dry_run
                )
            
            progress_bar.progress(100)
            status_text.text("✅ Investigation complete!")
            
            st.session_state.investigation_result = result
            st.session_state.selected_phase = 'overview'
            
            time.sleep(1)
            st.rerun()
            
        except Exception as e:
            status_text.text("")
            progress_bar.empty()
            st.error(f"❌ Investigation failed: {str(e)}")
            
            with st.expander("🔍 Error Details"):
                import traceback
                st.code(traceback.format_exc())
            
        finally:
            st.session_state.running = False


# ============================================================================
# DISPLAY RESULTS
# ============================================================================

if st.session_state.investigation_result:
    result = st.session_state.investigation_result
    selected = st.session_state.selected_phase
    
    # ========================================================================
    # OVERVIEW PAGE
    # ========================================================================
    
    if selected == 'overview':
        st.markdown("## 📊 Investigation Overview")
        
        # Key Results
        phase2_data = result.get('phases', {}).get('phase2', {}).get('data', {})
        phase4_data = result.get('phases', {}).get('phase4', {}).get('data', {})
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            fraud_type = phase2_data.get('fraud_type', 'Unknown')
            st.metric("Fraud Type", fraud_type)
        
        with col2:
            risk_level = phase2_data.get('risk_level', 'Unknown')
            st.metric("Risk Level", risk_level.upper())
        
        with col3:
            if phase4_data:
                fraud_status = phase4_data.get('fraud_status', 'Pending')
                st.metric("Classification", fraud_status)
            else:
                st.metric("Classification", "Pending")
        
        with col4:
            if phase4_data:
                confidence = phase4_data.get('confidence', 0)
                st.metric("Confidence", f"{confidence:.0%}")
            else:
                st.metric("Confidence", "N/A")
        
        # Performance Summary
        st.markdown("### ⏱️ Performance Summary")
        
        timeline = result.get('timeline', [])
        if timeline:
            total_time = sum(t.get('time_ms', 0) for t in timeline)
            
            col1, col2 = st.columns(2)
            
            with col1:
                for entry in timeline:
                    phase_name = entry.get('phase', 'Unknown')
                    phase_time = entry.get('time_ms', 0)
                    st.write(f"**{phase_name}:** {phase_time:.0f}ms")
                
                st.write(f"**Total:** {total_time:.0f}ms")
            
            with col2:
                # Query optimization summary
                phase3_data = result.get('phases', {}).get('phase3', {}).get('data', {})
                exec_log = phase3_data.get('execution_log', {}) if phase3_data else {}
                
                if exec_log:
                    stages_run = len(exec_log.get('stages_executed', []))
                    stages_skip = len(exec_log.get('stages_skipped', []))
                    
                    st.write(f"**SQL Stages Executed:** {stages_run}/5")
                    st.write(f"**SQL Stages Skipped:** {stages_skip}/5")
                    
                    if stages_skip > 0:
                        st.success(f"💰 Saved {stages_skip} expensive queries!")
        
        # Investigation Route
        st.markdown("### 🛣️ Investigation Route")
        
        route = phase2_data.get('investigation_route', 'Unknown')
        st.info(f"**Route:** {route}")
        
        reasoning = phase2_data.get('reasoning', 'No reasoning available')
        st.write(f"**Claude's Reasoning:** {reasoning}")
        
        # Key Evidence (if Phase 4 complete)
        if phase4_data and phase4_data.get('analysis'):
            st.markdown("### 🔍 Key Evidence")
            evidence = phase4_data.get('analysis', {}).get('key_evidence', [])
            for i, item in enumerate(evidence, 1):
                st.markdown(f"{i}. {item}")
        
        # Download complete results
        st.markdown("### 💾 Export Results")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.download_button(
                "📥 Download Full JSON",
                data=json.dumps(result, indent=2, default=str),
                file_name=f"investigation_{ticket_id}.json",
                mime="application/json"
            )
    
    # ========================================================================
    # PHASE-SPECIFIC PAGES
    # ========================================================================
    
    elif selected in ['phase1', 'phase2', 'phase3', 'phase4', 'phase5']:
        phase_data = result.get('phases', {}).get(selected, {})
        phase_info = phases[selected]
        
        st.markdown(f"## {phase_info['icon']} {phase_info['name']}")
        
        # Status
        status = phase_data.get('status', 'pending')
        if status == 'success':
            st.success(f"✅ Status: Complete ({phase_data.get('time_ms', 0):.0f}ms)")
        elif status == 'skipped':
            st.warning(f"⏭️ Status: Skipped - {phase_data.get('reason', 'Not executed')}")
        elif status == 'failed':
            st.error("❌ Status: Failed")
        
        # Tabs
        tab1, tab2, tab3 = st.tabs(["📊 Output", "🔧 Process", "📈 Analytics"])
        
        # OUTPUT TAB
        with tab1:
            if selected == 'phase2':
                # Query Planning Output
                data = phase_data.get('data', {})
                
                if data:
                    st.markdown("### 🔍 Identifiers Extracted")
                    
                    identifiers = data.get('identifiers_extracted', {})
                    found_count = sum(1 for v in identifiers.values() if v)
                    
                    st.write(f"**Found {found_count} identifiers:**")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        for key in ['IMEI', 'AccountNumber', 'LoanID']:
                            val = identifiers.get(key)
                            if val:
                                st.success(f"✅ {key}: {val}")
                            else:
                                st.write(f"○ {key}: Not found")
                    
                    with col2:
                        for key in ['PhoneNumber', 'DeviceID', 'CustomerID', 'AccountID']:
                            val = identifiers.get(key)
                            if val:
                                st.success(f"✅ {key}: {val}")
                            else:
                                st.write(f"○ {key}: Not found")
                    
                    st.markdown("### 🎯 Query Execution Plan")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Fraud Type:** {data.get('fraud_type')}")
                        st.write(f"**Risk Level:** {data.get('risk_level')}")
                        st.write(f"**Route:** {data.get('investigation_route')}")
                        st.write(f"**Device Model:** {data.get('device_model', 'Not specified')}")
                        st.write(f"**Supports DFRS:** {data.get('supports_dfrs', False)}")
                    
                    with col2:
                        st.write("**SQL Stages:**")
                        st.write(f"{'✅' if True else '❌'} Stage 1-2 (Basic)")
                        st.write(f"{'✅' if data.get('execute_stage_3_dfrs') else '⏭️'} Stage 3 (DFRS)")
                        st.write(f"{'✅' if data.get('execute_stage_4_history') else '⏭️'} Stage 4 (History)")
                        st.write(f"{'✅' if data.get('execute_stage_5_behavioral') else '⏭️'} Stage 5 (Behavioral)")
                    
                    st.markdown("### 💭 Claude's Reasoning")
                    st.info(data.get('reasoning', 'No reasoning provided'))
                    
                    st.markdown(f"**Estimated Time:** {data.get('estimated_time_ms', 0)}ms")
            
            elif selected == 'phase3':
                # SQL Execution Output
                data = phase_data.get('data', {})
                
                if data:
                    # Account Profile
                    account = data.get('account_profile', {})
                    if account:
                        st.markdown("### 📋 Account Profile (Stage 1-2)")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Account", account.get('AccountNumber', 'N/A'))
                            st.metric("Device", account.get('BrandModel', 'N/A'))
                        
                        with col2:
                            st.metric("IMEI", account.get('IMEI', 'N/A'))
                            st.metric("Loan Status", account.get('SystemLoanStatus', 'N/A'))
                        
                        with col3:
                            st.metric("Product", account.get('ProductSubCategory', 'N/A'))
                            supports_dfrs = account.get('SupportsDFRS', 0)
                            st.metric("DFRS Support", "Yes" if supports_dfrs else "No")
                    
                    # DFRS Signals
                    dfrs = data.get('dfrs_signals', {})
                    if dfrs:
                        st.markdown("### ⚠️ DFRS Signals (Stage 3)")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            fraud_score = dfrs.get('FraudScore', 0)
                            st.metric("Fraud Score", f"{fraud_score:.2f}")
                            if fraud_score > 0.7:
                                st.error("🔴 CRITICAL (>0.7)")
                            elif fraud_score > 0.5:
                                st.warning("🟡 WARNING (>0.5)")
                            else:
                                st.success("🟢 OK")
                        
                        with col2:
                            tamper_score = dfrs.get('HighestTamperScore', 0)
                            st.metric("Tamper Score", f"{tamper_score:.2f}")
                            if tamper_score > 0.9:
                                st.error("🔴 CRITICAL (>0.9)")
                            elif tamper_score > 0.6:
                                st.warning("🟡 WARNING (>0.6)")
                            else:
                                st.success("🟢 OK")
                        
                        with col3:
                            zero_days = dfrs.get('ZeroCreditDaysConsecutive', 0)
                            st.metric("Zero Credit Days", zero_days)
                            if zero_days > 30:
                                st.warning("⚠️ Payment evasion (>30)")
                        
                        st.write(f"**Tamper Reason:** {dfrs.get('TamperReason', 'None')}")
                        st.write(f"**Risk Segment:** {dfrs.get('FraudRiskSegment', 'Unknown')}")
                    
                    # Historical Tickets
                    history = data.get('historical_tickets', [])
                    if history:
                        st.markdown(f"### 📜 Historical Tickets (Stage 4): {len(history)} found")
                        
                        for i, ticket in enumerate(history[:5], 1):
                            ticket_id = ticket.get('TicketId', 'N/A')
                            subject = ticket.get('Subject', 'No subject')
                            
                            # Truncate long subjects for expander title
                            subject_short = subject[:50] + "..." if len(subject) > 50 else subject
                            
                            with st.expander(f"#{ticket_id} - {subject_short}"):
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    created = ticket.get('CreatedTime', 'N/A')
                                    status = ticket.get('Status', 'N/A')
                                    priority = ticket.get('Priority', 'N/A')
                                    
                                    st.write(f"**Created:** {created}")
                                    st.write(f"**Status:** {status}")
                                    st.write(f"**Priority:** {priority}")
                                
                                with col2:
                                    reason = ticket.get('ReasonForInteraction', 'N/A')
                                    subreason = ticket.get('SubReasonForInteraction', 'N/A')
                                    updated = ticket.get('LastUpdatedTime', 'N/A')
                                    
                                    st.write(f"**Reason:** {reason}")
                                    st.write(f"**Sub-reason:** {subreason}")
                                    st.write(f"**Updated:** {updated}")
                                
                                # Show description if available
                                description = ticket.get('description', '')
                                if description:
                                    desc_preview = description[:200] + "..." if len(description) > 200 else description
                                    st.write(f"**Description:** {desc_preview}")
                        
                        if len(history) > 5:
                            st.info(f"ℹ️ Showing first 5 of {len(history)} tickets. Export JSON to see all.")
                    else:
                        st.info("ℹ️ No historical tickets found for this account")
                    
                    # Behavioral Analysis
                    behavioral = data.get('behavioral_analysis', {})
                    if behavioral:
                        st.markdown("### 🚨 Behavioral Analysis (Stage 5)")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if behavioral.get('ResetPinFromNewDevice'):
                                st.error("🔴 CRITICAL: PIN Reset from New Device!")
                                st.write("Account takeover indicator")
                            
                            if behavioral.get('MultipleDevicesIncludingSuspicious'):
                                st.error("🔴 CRITICAL: Linked to Suspicious Device!")
                                st.write("Fraud ring connection detected")
                        
                        with col2:
                            if behavioral.get('CashLoanTaken'):
                                st.warning(f"💰 Cash Loan Taken: KES {behavioral.get('FulfilledCashLoanAmount', 0)}")
                            
                            st.write(f"**Unique Installations:** {behavioral.get('UniqueInstallations', 0)}")
                    
                    # Execution Log
                    exec_log = data.get('execution_log', {})
                    if exec_log:
                        st.markdown("### ⏱️ Execution Summary")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Stages Executed:**")
                            for stage in exec_log.get('stages_executed', []):
                                st.write(f"- {stage['stage']}: {stage['time_ms']:.0f}ms")
                        
                        with col2:
                            skipped = exec_log.get('stages_skipped', [])
                            if skipped:
                                st.write("**Stages Skipped:**")
                                for stage in skipped:
                                    st.write(f"- {stage}")
                        
                        st.write(f"**Total SQL Time:** {exec_log.get('total_time_ms', 0):.0f}ms")
            
            elif selected == 'phase4':
                # Investigation Results
                data = phase_data.get('data', {})
                
                if data and 'fraud_status' in data:
                    st.markdown("### 🎯 Fraud Classification")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        fraud_status = data.get('fraud_status')
                        confidence = data.get('confidence', 0)
                        
                        if fraud_status == 'Likely fraud':
                            st.markdown('<div class="danger-metric"><b>Classification:</b><br>Likely Fraud</div>', unsafe_allow_html=True)
                        else:
                            st.markdown('<div class="success-metric"><b>Classification:</b><br>Not Fraud</div>', unsafe_allow_html=True)
                        
                        st.progress(confidence, text=f"Confidence: {confidence:.0%}")
                    
                    with col2:
                        updates = data.get('updates', {}).get('custom_fields', {})
                        case_outcome = updates.get('case_outcome', 'N/A')
                        st.info(f"**Case Outcome:**\n\n{case_outcome}")
                    
                    with col3:
                        allegation = updates.get('primary_allegation')
                        st.info(f"**Primary Allegation:**\n\n{allegation or 'None'}")
                    
                    # Suspect Information
                    suspect_type = updates.get('suspect_type')
                    if suspect_type:
                        st.markdown("### 👤 Suspect Information")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.write(f"**Type:** {suspect_type}")
                        with col2:
                            st.write(f"**Name:** {updates.get('suspect_name', 'Not found')}")
                        with col3:
                            st.write(f"**Phone:** {updates.get('suspect_number', 'Not found')}")
                    
                    # Key Evidence
                    st.markdown("### 🔍 Key Evidence")
                    evidence = data.get('analysis', {}).get('key_evidence', [])
                    for i, item in enumerate(evidence, 1):
                        st.markdown(f"{i}. {item}")
                    
                    # Risk Factors
                    risk_factors = data.get('analysis', {}).get('risk_factors', {})
                    if risk_factors:
                        st.markdown("### ⚠️ Risk Factors Detected")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if risk_factors.get('dfrs_fraud_score'):
                                st.write(f"DFRS Fraud Score: {risk_factors['dfrs_fraud_score']:.2f}")
                            if risk_factors.get('dfrs_tamper_score'):
                                st.write(f"DFRS Tamper Score: {risk_factors['dfrs_tamper_score']:.2f}")
                            if risk_factors.get('payment_evasion'):
                                st.error("💳 Payment Evasion Detected")
                        
                        with col2:
                            if risk_factors.get('behavioral_account_takeover'):
                                st.error("🚨 Account Takeover Risk!")
                            if risk_factors.get('behavioral_fraud_ring'):
                                st.error("🚨 Fraud Ring Connection!")
                            if risk_factors.get('repeat_offender'):
                                st.warning("🔁 Repeat Offender Pattern")
                    
                    # Data Sources Used
                    data_sources = data.get('analysis', {}).get('data_sources_used', {})
                    if data_sources:
                        st.markdown("### 📊 Data Sources Used")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            if data_sources.get('dfrs_available'):
                                st.success("✅ DFRS Signals")
                            else:
                                st.write("○ DFRS: Not available")
                        
                        with col2:
                            if data_sources.get('behavioral_available'):
                                st.success("✅ Behavioral Analysis")
                            else:
                                st.write("○ Behavioral: Not available")
                        
                        with col3:
                            hist_count = data_sources.get('historical_count', 0)
                            if hist_count > 0:
                                st.success(f"✅ Historical: {hist_count} tickets")
                            else:
                                st.write("○ Historical: No tickets")
        
        # PROCESS TAB
        with tab2:
            st.markdown("### 🔧 How This Phase Works")
            
            if selected == 'phase2':
                st.markdown("""
**Purpose:** Query Planning - Claude decides which SQL stages to run

**Process:**
1. Read entire ticket (subject, description, case details, conversations)
2. Extract ALL identifiers (IMEI, phone, account, loan ID, etc.)
3. Classify fraud type from ticket content
4. Assess risk level
5. **Decide which SQL stages to execute** (key innovation!)
6. Create execution plan

**Decision Logic:**
- Device tampering → Run DFRS (Stage 3)
- Account takeover → Run Behavioral (Stage 5)
- External scam → Skip device queries
- Unknown → Conservative approach

**Model:** Claude Sonnet 4.5
**Time:** ~500ms
**Cost:** ~$0.002
                """)
            
            elif selected == 'phase3':
                st.markdown("""
**Purpose:** Dynamic SQL Execution - Run only necessary queries

**5 Conditional Stages:**

**Stage 1-2: Basic Account** (Always)
- Get account profile, device info
- Time: ~100ms

**Stage 3: DFRS** (If device supports it)
- Device fraud signals, tampering scores
- Time: ~200ms

**Stage 4: Historical** (Usually)
- Previous tickets for this account
- Time: ~50ms

**Stage 5: Behavioral** (Rarely - expensive!)
- Account takeover, fraud ring detection
- Time: 2-5 seconds

**Key Innovation:**
Only runs expensive queries when needed!

**Result:**
- 60% of cases: <500ms (skip Stage 5)
- 10% of cases: 4-5s (run all stages for critical cases)
                """)
            
            elif selected == 'phase4':
                st.markdown("""
**Purpose:** AI Fraud Investigation with Enhanced Data

**Analyzes:**
1. **DFRS Signals** (if available)
   - Fraud score > 0.7 → Critical
   - Tamper score > 0.9 → Confirmed tampering
   - Zero credit days > 30 → Payment evasion

2. **Behavioral Flags** (if available)
   - PIN reset from new device → Account takeover
   - Linked to suspicious device → Fraud ring
   - **These are VERY strong signals**

3. **Historical Patterns**
   - 3+ fraud tickets → Repeat offender
   - Same issue → Ongoing vs new

4. **Ticket Narrative**
   - Extract suspect info
   - Identify fraud patterns
   - Match training examples

**Output:**
- Fraud classification
- Confidence score
- Freshservice field updates
- Detailed reasoning

**Model:** Claude Sonnet 4.5
**Time:** ~600ms
**Cost:** ~$0.003
                """)
        
        # ANALYTICS TAB
        with tab3:
            if selected == 'phase3':
                data = phase_data.get('data', {})
                dfrs = data.get('dfrs_signals', {})
                
                if dfrs:
                    # Check if pandas is available
                    try:
                        import pandas as pd
                        PANDAS_AVAILABLE = True
                    except ImportError:
                        PANDAS_AVAILABLE = False
                    
                    st.markdown("### 📊 DFRS Signal Visualization")
                    
                    if PANDAS_AVAILABLE:
                        # Scores
                        scores_data = {
                            'Metric': ['Fraud Score', 'Tamper Score'],
                            'Value': [dfrs.get('FraudScore', 0), dfrs.get('HighestTamperScore', 0)],
                            'Threshold': [0.7, 0.9]
                        }
                        
                        df_scores = pd.DataFrame(scores_data)
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.bar_chart(df_scores.set_index('Metric')['Value'])
                        
                        with col2:
                            st.write("**Interpretation:**")
                            fraud_score = dfrs.get('FraudScore', 0)
                            tamper_score = dfrs.get('HighestTamperScore', 0)
                            
                            if fraud_score > 0.7 or tamper_score > 0.9:
                                st.error("🔴 HIGH RISK - Critical fraud indicators")
                            elif fraud_score > 0.5 or tamper_score > 0.6:
                                st.warning("🟡 MEDIUM RISK - Investigation warranted")
                            else:
                                st.success("🟢 LOW RISK - No critical indicators")
                    else:
                        # Show data without pandas
                        st.warning("📊 Install pandas for chart visualization: `pip install pandas`")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            fraud_score = dfrs.get('FraudScore', 0)
                            st.metric("Fraud Score", f"{fraud_score:.2f}")
                            tamper_score = dfrs.get('HighestTamperScore', 0)
                            st.metric("Tamper Score", f"{tamper_score:.2f}")
                        
                        with col2:
                            st.write("**Interpretation:**")
                            
                            if fraud_score > 0.7 or tamper_score > 0.9:
                                st.error("🔴 HIGH RISK - Critical fraud indicators")
                            elif fraud_score > 0.5 or tamper_score > 0.6:
                                st.warning("🟡 MEDIUM RISK - Investigation warranted")
                            else:
                                st.success("🟢 LOW RISK - No critical indicators")

else:
    # No investigation yet
    st.info("👆 Enter a ticket ID and click 'Run Dynamic Investigation'")
    
    st.markdown("### 🎯 About This System")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
**Dynamic Query Optimization:**

- Claude reads ticket first
- Decides which data to fetch
- Skips irrelevant expensive queries
- **Result: 47% cost savings**

**5 Phases:**
1. Fetch Freshservice
2. Query Planning (Claude)
3. Dynamic SQL (1-5 stages)
4. Fraud Investigation (Claude)
5. Update Freshservice
        """)
    
    with col2:
        st.markdown("""
**Performance:**

- 60% of cases: <500ms
- 30% of cases: 500ms-2s
- 10% of cases: 2-5s (fraud rings)

**Data Sources:**
- DFRS (device fraud signals)
- Behavioral (account takeover)
- Historical (repeat patterns)
- Ticket narrative

**Cost:** ~$0.005/investigation
        """)


# Footer
st.markdown("---")
st.markdown("**M-KOPA Dynamic Fraud Investigation** | Claude-Powered Query Optimization | Demo v1.0")
