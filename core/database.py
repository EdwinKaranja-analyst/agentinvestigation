"""
M-KOPA Fraud Investigation - Database Operations
SQLite database for caching investigation results

You rarely need to edit this unless:
- Adding new database tables
- Changing cache structure
- Adding new query functions
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, Optional

from config.settings import CACHE_DB_PATH


# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def init_database():
    """Initialize SQLite cache database with enhanced schema"""
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    
    # Create investigations table with full data storage
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS investigations (
            ticket_id TEXT PRIMARY KEY,
            investigation_date TIMESTAMP NOT NULL,
            result_file TEXT,
            fraud_status TEXT,
            confidence REAL,
            fraud_type TEXT,
            risk_level TEXT,
            case_outcome TEXT,
            primary_allegation TEXT,
            suspect_type TEXT,
            suspect_name TEXT,
            total_time_ms REAL,
            phases_executed INTEGER,
            success INTEGER,
            full_investigation_data TEXT
        )
    """)
    
    # Create indexes for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_investigation_date 
        ON investigations(investigation_date)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_fraud_status 
        ON investigations(fraud_status)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_confidence 
        ON investigations(confidence)
    """)
    
    conn.commit()
    conn.close()
    
    return CACHE_DB_PATH


# ============================================================================
# SAVE & RETRIEVE OPERATIONS
# ============================================================================

def save_investigation(ticket_id: str, result: Dict[str, Any], result_file: Optional[str] = None):
    """
    Save investigation result to database
    
    Args:
        ticket_id: Ticket ID
        result: Complete investigation result dictionary
        result_file: Optional file path (for legacy compatibility)
    """
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    
    # Extract key fields from result
    phase2_data = result.get('phases', {}).get('phase2', {}).get('data', {})
    phase4_data = result.get('phases', {}).get('phase4', {}).get('data', {})
    
    fraud_status = phase4_data.get('fraud_status')
    confidence = phase4_data.get('confidence', 0.0)
    fraud_type = phase2_data.get('fraud_type')
    risk_level = phase2_data.get('risk_level')
    case_outcome = phase4_data.get('updates', {}).get('custom_fields', {}).get('case_outcome')
    primary_allegation = phase4_data.get('primary_allegation')
    suspect_type = phase4_data.get('updates', {}).get('custom_fields', {}).get('suspect_type')
    suspect_name = phase4_data.get('updates', {}).get('custom_fields', {}).get('suspect_name')
    
    # Calculate total time
    timeline = result.get('timeline', [])
    total_time = sum(t.get('time_ms', 0) for t in timeline)
    
    # Count phases executed
    phases_executed = len([p for p in result.get('phases', {}).values() if p.get('status') == 'success'])
    
    success = 1 if result.get('success') else 0
    
    # Store full investigation data as JSON for later retrieval
    full_data_json = json.dumps(result, default=str)
    
    cursor.execute("""
        INSERT OR REPLACE INTO investigations 
        (ticket_id, investigation_date, result_file, fraud_status, confidence,
         fraud_type, risk_level, case_outcome, total_time_ms, phases_executed, success,
         primary_allegation, suspect_type, suspect_name, full_investigation_data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        ticket_id,
        datetime.utcnow().isoformat(),
        result_file or 'database_only',
        fraud_status,
        confidence,
        fraud_type,
        risk_level,
        case_outcome,
        total_time,
        phases_executed,
        success,
        primary_allegation,
        suspect_type,
        suspect_name,
        full_data_json
    ))
    
    conn.commit()
    conn.close()


def check_if_investigated(ticket_id: str) -> Optional[Dict[str, Any]]:
    """
    Check if ticket was already investigated
    
    Args:
        ticket_id: Ticket ID to check
    
    Returns:
        Investigation record if found, None otherwise
    """
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            ticket_id,
            investigation_date,
            result_file,
            fraud_status,
            confidence,
            fraud_type,
            case_outcome
        FROM investigations
        WHERE ticket_id = ?
    """, (ticket_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {
            'ticket_id': row[0],
            'investigation_date': row[1],
            'result_file': row[2],
            'fraud_status': row[3],
            'confidence': row[4],
            'fraud_type': row[5],
            'case_outcome': row[6]
        }
    return None


def get_investigation(ticket_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve full investigation data for a ticket
    
    Args:
        ticket_id: Ticket ID
    
    Returns:
        Complete investigation result dictionary or None
    """
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT full_investigation_data
        FROM investigations
        WHERE ticket_id = ?
    """, (ticket_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if row and row[0]:
        return json.loads(row[0])
    return None


def get_cache_stats() -> Dict[str, Any]:
    """Get statistics from cache database"""
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    
    # Total investigations
    cursor.execute("SELECT COUNT(*) FROM investigations")
    total = cursor.fetchone()[0]
    
    # Fraud counts
    cursor.execute("SELECT COUNT(*) FROM investigations WHERE fraud_status = 'Likely fraud'")
    fraud_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM investigations WHERE fraud_status = 'Not fraud'")
    not_fraud_count = cursor.fetchone()[0]
    
    # Average confidence
    cursor.execute("SELECT AVG(confidence) FROM investigations WHERE confidence IS NOT NULL")
    avg_confidence = cursor.fetchone()[0] or 0.0
    
    conn.close()
    
    return {
        'total_investigations': total,
        'fraud_detected': fraud_count,
        'not_fraud': not_fraud_count,
        'average_confidence': avg_confidence
    }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def clear_cache():
    """Clear all cached investigations"""
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM investigations")
    conn.commit()
    conn.close()
    
    print(f"✅ Cache cleared")


def delete_investigation(ticket_id: str):
    """Delete specific investigation from cache"""
    
    conn = sqlite3.connect(CACHE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM investigations WHERE ticket_id = ?", (ticket_id,))
    conn.commit()
    conn.close()
    
    print(f"✅ Deleted investigation for ticket #{ticket_id}")
