"""
M-KOPA Fraud Investigation - External API Connections
Handles Freshservice API and Azure Synapse connections

You typically won't need to edit this file unless:
- Changing API endpoints
- Adding new data sources
- Modifying connection logic
"""

import os
import requests
import pyodbc
import struct
from datetime import datetime
from typing import Dict, Any, Optional

try:
    from azure.identity import AzureCliCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

from config.settings import (
    FRESHSERVICE_BASE_URL,
    SYNAPSE_SERVER,
    SYNAPSE_DATABASE
)


# ============================================================================
# FRESHSERVICE API
# ============================================================================

def fetch_freshservice_ticket(ticket_id: str) -> Dict[str, Any]:
    """
    Fetch ticket data from Freshservice API
    
    Args:
        ticket_id: Freshservice ticket ID
    
    Returns:
        Ticket data dictionary with basic_data and conversations
    """
    
    API_KEY = os.getenv('FRESHSERVICE_API_KEY')
    
    if not API_KEY:
        raise ValueError("FRESHSERVICE_API_KEY environment variable not set")
    
    try:
        response = requests.get(
            f"{FRESHSERVICE_BASE_URL}/tickets/{ticket_id}?include=conversations",
            auth=(API_KEY, 'X'),
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"API call failed with status {response.status_code}: {response.text}")
        
        ticket_data = response.json()
        
        if 'ticket' in ticket_data:
            ticket_info = ticket_data['ticket']
            conversations = ticket_data.get('conversations', [])
        else:
            ticket_info = ticket_data
            conversations = []
        
        return {
            "ticket_id": ticket_id,
            "basic_data": ticket_info,
            "conversations": conversations,
            "fetch_timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise Exception(f"Failed to fetch ticket {ticket_id}: {str(e)}")


def fetch_open_fraud_tickets() -> list:
    """
    Fetch all open fraud tickets from Freshservice
    
    Returns:
        List of ticket IDs
    """
    
    API_KEY = os.getenv('FRESHSERVICE_API_KEY')
    
    if not API_KEY:
        raise ValueError("FRESHSERVICE_API_KEY environment variable not set")
    
    try:
        # Fetch tickets assigned to Fraud Team that are Open or Pending
        params = {
            'query': '"group_id:27000198468 AND (status:2 OR status:3)"'
        }
        
        response = requests.get(
            f"{FRESHSERVICE_BASE_URL}/tickets",
            auth=(API_KEY, 'X'),
            params=params,
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"API failed: {response.status_code} - {response.text}")
        
        data = response.json()
        tickets = data.get('tickets', [])
        
        ticket_ids = [str(ticket['id']) for ticket in tickets]
        
        return ticket_ids
        
    except Exception as e:
        raise Exception(f"Failed to fetch open tickets: {str(e)}")


def update_freshservice_ticket(ticket_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update Freshservice ticket with investigation results
    
    Args:
        ticket_id: Ticket ID to update
        updates: Dictionary of fields to update
    
    Returns:
        Update status
    """
    
    # TODO: Implement actual update logic when needed
    # For now, just return success in dry run
    
    return {
        'success': True,
        'ticket_id': ticket_id,
        'dry_run': True
    }


# ============================================================================
# AZURE SYNAPSE CONNECTION
# ============================================================================

def get_synapse_connection():
    """
    Connect to Azure Synapse using Azure CLI credentials
    
    Returns:
        pyodbc connection object
    """
    
    if not AZURE_AVAILABLE:
        raise Exception("Azure libraries not available. Install: pip install azure-identity pyodbc")
    
    credential = AzureCliCredential()
    databaseToken = credential.get_token('https://database.windows.net/')
    tokenb = bytes(databaseToken[0], "UTF-8")
    exptoken = b''
    for i in tokenb:
        exptoken += bytes({i})
        exptoken += bytes(1)
    tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
    connString = f"Driver={{ODBC Driver 17 for SQL Server}};SERVER={SYNAPSE_SERVER};DATABASE={SYNAPSE_DATABASE};"
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    
    connection = pyodbc.connect(connString, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: tokenstruct})
    return connection
