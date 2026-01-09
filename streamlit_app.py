"""
Simple Streamlit UI for fraud investigations
Updated with priority and case helper comment display
"""

import streamlit as st
import json
from engine import investigate_ticket

st.set_page_config(page_title="M-KOPA Fraud Investigation", page_icon="🔍")

st.title("🔍 M-KOPA Fraud Investigation")
st.markdown("---")

# Input
col1, col2, col3 = st.columns([3, 1, 1])

with col1:
    ticket_id = st.text_input("Ticket ID", placeholder="e.g., 151333")

with col2:
    use_cache = st.checkbox("Use cache", value=True)

with col3:
    update_fs = st.checkbox("Update FS", value=True, help="Update Freshservice ticket")

if st.button("🔍 Investigate", type="primary", use_container_width=True):
    if not ticket_id:
        st.error("Please enter a ticket ID")
    else:
        with st.spinner("Investigating..."):
            result = investigate_ticket(ticket_id, use_cache=use_cache, update_freshservice=update_fs)
        
        if result.get('success'):
            # Check for wrong escalation
            if result.get('wrong_escalation'):
                st.warning("⚠️ Wrong Escalation Detected")
                st.write(result.get('query_plan', {}).get('reasoning', 'Not a fraud case'))
            else:
                st.success("✅ Investigation Complete")
            
            # Results
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                status = result.get('fraud_status', 'Unknown')
                if status == 'Confirmed fraud':
                    st.error(f"**Status:** {status}")
                elif status == 'Likely fraud':
                    st.warning(f"**Status:** {status}")
                else:
                    st.success(f"**Status:** {status}")
            
            with col2:
                conf = result.get('confidence', 0)
                st.metric("Confidence", f"{conf:.0%}")
            
            with col3:
                priority_label = result.get('priority_label', 'N/A')
                priority_colors = {
                    'Critical': '🔴',
                    'High': '🟠', 
                    'Medium': '🟡',
                    'Low': '🟢'
                }
                icon = priority_colors.get(priority_label, '⚪')
                st.metric("Priority", f"{icon} {priority_label}")
            
            with col4:
                outcome = result.get('case_outcome', 'N/A')
                st.info(f"**Outcome:** {outcome}")
            
            # Freshservice update status
            if result.get('freshservice_updated'):
                st.success("✅ Freshservice updated with priority and case comment")
            elif update_fs:
                st.warning("⚠️ Freshservice update failed")
            
            # Case Helper Comment
            if not result.get('wrong_escalation'):
                st.markdown("### 📋 Case Helper Comment")
                with st.expander("View case helper comment (internal note)", expanded=False):
                    st.text(result.get('case_helper_comment', 'No comment'))
            
            # Summary
            if not result.get('wrong_escalation'):
                st.markdown("### 📝 Investigation Summary")
                st.write(result.get('investigation_summary', 'No summary'))
                
                # Evidence
                evidence = result.get('key_evidence', [])
                if evidence:
                    st.markdown("### 🔍 Key Evidence")
                    for i, item in enumerate(evidence, 1):
                        st.write(f"{i}. {item}")
            
            # Phases
            with st.expander("🔧 Investigation Phases"):
                phases = result.get('phases', {})
                
                # Query plan
                plan = phases.get('planning')
                if plan:
                    st.markdown("**Query Plan:**")
                    st.json({
                        'investigation_subject': plan.get('investigation_subject'),
                        'fraud_type': plan.get('fraud_type'),
                        'primary_allegation': plan.get('primary_allegation'),
                        'execute_dfrs': plan.get('execute_dfrs'),
                        'execute_history': plan.get('execute_history')
                    })
                
                # Account
                account = phases.get('account')
                if account:
                    st.markdown("**Account Data:**")
                    st.json({
                        'AccountNumber': account.get('AccountNumber'),
                        'BrandModel': account.get('BrandModel'),
                        'IMEI': account.get('IMEI'),
                        'SupportsDFRS': account.get('SupportsDFRS')
                    })
                
                # DFRS
                dfrs = phases.get('dfrs')
                if dfrs:
                    st.markdown("**DFRS Signals:**")
                    st.json({
                        'FraudScore': dfrs.get('FraudScore'),
                        'TamperScore': dfrs.get('TamperScore'),
                        'ZeroCreditDaysConsecutive': dfrs.get('ZeroCreditDaysConsecutive'),
                        'TamperReason': dfrs.get('TamperReason')
                    })
                
                # History
                history = phases.get('history', [])
                if history:
                    st.markdown(f"**Historical Tickets:** {len(history)} found")
                    for ticket in history[:3]:
                        st.write(f"- #{ticket['TicketId']}: {ticket['Subject']}")
            
            # Download
            st.download_button(
                "📥 Download JSON",
                data=json.dumps(result, indent=2, default=str),
                file_name=f"investigation_{ticket_id}.json",
                mime="application/json"
            )
        
        else:
            st.error(f"❌ Investigation failed: {result.get('error')}")

# Sidebar
with st.sidebar:
    st.markdown("## About")
    st.markdown("""
    **M-KOPA Fraud Investigation v2.1**
    
    Using:
    - Claude Sonnet 4.5
    - Azure Synapse
    - Freshservice API
    
    **New in v2.1:**
    - ✅ Auto-update Freshservice
    - 🎯 Priority calculation
    - 📋 Case helper comments
    - ⚠️ Wrong escalation flagging
    
    **How it works:**
    1. Fetch ticket
    2. Plan queries
    3. Fetch data
    4. Analyze
    5. Calculate priority
    6. Update Freshservice
    """)
    
    st.markdown("---")
    
    st.markdown("### Priority Levels")
    st.markdown("""
    - 🔴 **Critical (4)**: Cash loan fraud, suspect identified
    - 🟠 **High (3)**: Confirmed fraud, DSR theft
    - 🟡 **Medium (2)**: Likely fraud
    - 🟢 **Low (1)**: Not fraud, wrong escalation
    """)
    
    st.markdown("---")
    
    st.markdown("### Quick Test")
    st.code("python engine.py 151333")
    st.code("python batch_runner.py --update 151333 151334")