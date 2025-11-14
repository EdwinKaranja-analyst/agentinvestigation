"""
Simple Streamlit UI for fraud investigations
"""

import streamlit as st
import json
from engine import investigate_ticket

st.set_page_config(page_title="M-KOPA Fraud Investigation", page_icon="🔍")

st.title("🔍 M-KOPA Fraud Investigation")
st.markdown("---")

# Input
col1, col2 = st.columns([3, 1])

with col1:
    ticket_id = st.text_input("Ticket ID", placeholder="e.g., 151333")

with col2:
    use_cache = st.checkbox("Use cache", value=True)

if st.button("🔍 Investigate", type="primary", use_container_width=True):
    if not ticket_id:
        st.error("Please enter a ticket ID")
    else:
        with st.spinner("Investigating..."):
            result = investigate_ticket(ticket_id, use_cache=use_cache)
        
        if result.get('success'):
            st.success("✅ Investigation Complete")
            
            # Results
            col1, col2, col3 = st.columns(3)
            
            with col1:
                status = result.get('fraud_status', 'Unknown')
                if status == 'Likely fraud':
                    st.error(f"**Status:** {status}")
                else:
                    st.success(f"**Status:** {status}")
            
            with col2:
                conf = result.get('confidence', 0)
                st.metric("Confidence", f"{conf:.0%}")
            
            with col3:
                outcome = result.get('case_outcome', 'N/A')
                st.info(f"**Outcome:** {outcome}")
            
            # Summary
            st.markdown("### 📋 Summary")
            st.write(result.get('summary', 'No summary'))
            
            # Evidence
            evidence = result.get('key_evidence', [])
            if evidence:
                st.markdown("### 🔍 Key Evidence")
                for i, item in enumerate(evidence, 1):
                    st.write(f"{i}. {item}")
            
            # Phases
            with st.expander("🔧 Investigation Phases"):
                phases = result.get('phases', {})
                
                # Account
                account = phases.get('account')
                if account:
                    st.markdown("**Account Data:**")
                    st.json(account)
                
                # DFRS
                dfrs = phases.get('dfrs')
                if dfrs:
                    st.markdown("**DFRS Signals:**")
                    st.json(dfrs)
                
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
    Simple fraud investigation system using:
    - Claude Sonnet 4.5
    - Azure Synapse
    - Freshservice API
    
    **How it works:**
    1. Fetch ticket
    2. Plan queries
    3. Fetch data
    4. Analyze
    """)
    
    st.markdown("---")
    st.markdown("### Quick Test")
    st.code("python engine.py 151333")