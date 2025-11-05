#!/usr/bin/env python3
"""
Explore Fraud Investigation Database
Easy Python interface to query your SQLite investigation results

Usage:
    python explore_database.py
"""

import sqlite3
import json
from datetime import datetime, timedelta

DB_PATH = "investigations_cache.db"


def view_all_investigations(limit=20):
    """View recent investigations"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            ticket_id,
            investigation_date,
            fraud_status,
            confidence,
            fraud_type,
            case_outcome
        FROM investigations
        ORDER BY investigation_date DESC
        LIMIT ?
    """, (limit,))
    
    print(f"\n📋 Recent Investigations (Last {limit})")
    print("="*100)
    print(f"{'Ticket':<10} {'Date':<20} {'Status':<15} {'Conf':<8} {'Type':<20} {'Outcome':<25}")
    print("-"*100)
    
    for row in cursor.fetchall():
        ticket, date, status, conf, ftype, outcome = row
        conf_str = f"{conf:.0%}" if conf else "N/A"
        print(f"{ticket:<10} {date:<20} {status:<15} {conf_str:<8} {ftype or 'N/A':<20} {outcome or 'N/A':<25}")
    
    conn.close()


def get_fraud_summary():
    """Get fraud detection summary"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Total counts
    cursor.execute("SELECT COUNT(*) FROM investigations")
    total = cursor.fetchone()[0]
    
    # Fraud breakdown
    cursor.execute("""
        SELECT fraud_status, COUNT(*), AVG(confidence)
        FROM investigations
        GROUP BY fraud_status
    """)
    
    print("\n📊 Fraud Detection Summary")
    print("="*70)
    print(f"Total investigations: {total}")
    print("\nBreakdown:")
    
    for row in cursor.fetchall():
        status, count, avg_conf = row
        pct = (count / total * 100) if total > 0 else 0
        conf_str = f"{avg_conf:.0%}" if avg_conf else "N/A"
        print(f"  {status or 'Unknown'}: {count} ({pct:.1f}%) - Avg confidence: {conf_str}")
    
    conn.close()


def get_fraud_by_type():
    """Breakdown by fraud type"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            fraud_type,
            COUNT(*) as count,
            SUM(CASE WHEN fraud_status = 'Likely fraud' THEN 1 ELSE 0 END) as fraud_count,
            AVG(confidence) as avg_conf
        FROM investigations
        WHERE fraud_type IS NOT NULL
        GROUP BY fraud_type
        ORDER BY count DESC
    """)
    
    print("\n📊 Fraud Type Breakdown")
    print("="*80)
    print(f"{'Type':<25} {'Total':<8} {'Fraud':<8} {'Fraud %':<10} {'Avg Conf':<10}")
    print("-"*80)
    
    for row in cursor.fetchall():
        ftype, total, fraud, avg_conf = row
        fraud_pct = (fraud / total * 100) if total > 0 else 0
        conf_str = f"{avg_conf:.0%}" if avg_conf else "N/A"
        print(f"{ftype:<25} {total:<8} {fraud:<8} {fraud_pct:<10.1f} {conf_str:<10}")
    
    conn.close()


def get_high_confidence_fraud():
    """Get high confidence fraud cases"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            ticket_id,
            fraud_type,
            confidence,
            case_outcome,
            suspect_type,
            investigation_date
        FROM investigations
        WHERE fraud_status = 'Likely fraud'
        AND confidence > 0.85
        ORDER BY confidence DESC
    """)
    
    print("\n🚨 High Confidence Fraud Cases (>85%)")
    print("="*100)
    print(f"{'Ticket':<10} {'Type':<20} {'Conf':<8} {'Outcome':<25} {'Suspect':<15} {'Date':<20}")
    print("-"*100)
    
    for row in cursor.fetchall():
        ticket, ftype, conf, outcome, suspect, date = row
        conf_str = f"{conf:.0%}"
        print(f"{ticket:<10} {ftype or 'N/A':<20} {conf_str:<8} {outcome or 'N/A':<25} {suspect or 'N/A':<15} {date:<20}")
    
    conn.close()


def get_full_investigation(ticket_id):
    """Retrieve full investigation data for a ticket"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT full_investigation_data
        FROM investigations
        WHERE ticket_id = ?
    """, (ticket_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if row and row[0]:
        data = json.loads(row[0])
        
        print(f"\n🔍 Full Investigation for Ticket #{ticket_id}")
        print("="*70)
        
        # Show key info
        phase4 = data.get('phases', {}).get('phase4', {}).get('data', {})
        
        print(f"\nClassification: {phase4.get('fraud_status')}")
        print(f"Confidence: {phase4.get('confidence'):.0%}")
        print(f"Type: {data.get('phases', {}).get('phase2', {}).get('data', {}).get('fraud_type')}")
        
        # Evidence
        evidence = phase4.get('analysis', {}).get('key_evidence', [])
        if evidence:
            print(f"\nKey Evidence:")
            for i, item in enumerate(evidence, 1):
                print(f"  {i}. {item}")
        
        # Historical tickets
        phase3 = data.get('phases', {}).get('phase3', {}).get('data', {})
        history = phase3.get('historical_tickets', [])
        if history:
            print(f"\nHistorical Tickets Found: {len(history)}")
            for ticket in history[:3]:
                print(f"  - #{ticket.get('TicketId')}: {ticket.get('Subject', 'N/A')[:50]}")
        
        return data
    else:
        print(f"❌ No investigation found for ticket #{ticket_id}")
        return None


def search_by_suspect(suspect_type=None, suspect_name=None):
    """Search investigations by suspect"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    query = "SELECT ticket_id, fraud_status, confidence, suspect_type, suspect_name FROM investigations WHERE 1=1"
    params = []
    
    if suspect_type:
        query += " AND suspect_type = ?"
        params.append(suspect_type)
    
    if suspect_name:
        query += " AND suspect_name LIKE ?"
        params.append(f"%{suspect_name}%")
    
    query += " ORDER BY investigation_date DESC"
    
    cursor.execute(query, params)
    
    print(f"\n🔍 Suspect Search Results")
    print("="*90)
    print(f"{'Ticket':<10} {'Status':<15} {'Conf':<8} {'Suspect Type':<20} {'Suspect Name':<25}")
    print("-"*90)
    
    for row in cursor.fetchall():
        ticket, status, conf, stype, sname = row
        conf_str = f"{conf:.0%}" if conf else "N/A"
        print(f"{ticket:<10} {status:<15} {conf_str:<8} {stype or 'N/A':<20} {sname or 'N/A':<25}")
    
    conn.close()


def get_today_stats():
    """Get today's investigation statistics"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    today = datetime.utcnow().date().isoformat()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN fraud_status = 'Likely fraud' THEN 1 ELSE 0 END) as fraud,
            AVG(confidence) as avg_conf,
            AVG(total_time_ms) as avg_time
        FROM investigations
        WHERE DATE(investigation_date) = ?
    """, (today,))
    
    row = cursor.fetchone()
    
    print(f"\n📅 Today's Statistics ({today})")
    print("="*70)
    
    if row and row[0] > 0:
        total, fraud, avg_conf, avg_time = row
        not_fraud = total - fraud
        
        print(f"Total investigated: {total}")
        print(f"Fraud detected: {fraud} ({fraud/total*100:.1f}%)")
        print(f"Not fraud: {not_fraud} ({not_fraud/total*100:.1f}%)")
        print(f"Average confidence: {avg_conf:.0%}" if avg_conf else "Average confidence: N/A")
        print(f"Average time: {avg_time:.0f}ms" if avg_time else "Average time: N/A")
    else:
        print("No investigations today yet")
    
    conn.close()


def export_to_csv(output_file="investigations_export.csv"):
    """Export all investigations to CSV"""
    
    import csv
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            ticket_id,
            investigation_date,
            fraud_status,
            confidence,
            fraud_type,
            risk_level,
            case_outcome,
            primary_allegation,
            suspect_type,
            suspect_name,
            total_time_ms
        FROM investigations
        ORDER BY investigation_date DESC
    """)
    
    rows = cursor.fetchall()
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Ticket ID', 'Investigation Date', 'Fraud Status', 'Confidence',
            'Fraud Type', 'Risk Level', 'Case Outcome', 'Primary Allegation',
            'Suspect Type', 'Suspect Name', 'Time (ms)'
        ])
        writer.writerows(rows)
    
    conn.close()
    
    print(f"✅ Exported {len(rows)} investigations to {output_file}")


# ============================================================================
# INTERACTIVE MENU
# ============================================================================

def main_menu():
    """Interactive menu for exploring database"""
    
    print("\n" + "="*70)
    print("🔍 FRAUD INVESTIGATION DATABASE EXPLORER")
    print("="*70)
    print(f"Database: {DB_PATH}")
    print()
    
    while True:
        print("\n📋 Options:")
        print("  1. View recent investigations")
        print("  2. Fraud detection summary")
        print("  3. Breakdown by fraud type")
        print("  4. High confidence fraud cases")
        print("  5. Today's statistics")
        print("  6. Search by suspect")
        print("  7. Get full investigation for ticket")
        print("  8. Export to CSV")
        print("  9. Run custom SQL query")
        print("  0. Exit")
        
        choice = input("\nEnter choice: ").strip()
        
        if choice == '1':
            limit = input("How many? (default 20): ").strip() or "20"
            view_all_investigations(int(limit))
        
        elif choice == '2':
            get_fraud_summary()
        
        elif choice == '3':
            get_fraud_by_type()
        
        elif choice == '4':
            get_high_confidence_fraud()
        
        elif choice == '5':
            get_today_stats()
        
        elif choice == '6':
            suspect_type = input("Suspect type (or Enter to skip): ").strip() or None
            suspect_name = input("Suspect name (or Enter to skip): ").strip() or None
            search_by_suspect(suspect_type, suspect_name)
        
        elif choice == '7':
            ticket_id = input("Ticket ID: ").strip()
            if ticket_id:
                get_full_investigation(ticket_id)
        
        elif choice == '8':
            filename = input("Output filename (default: investigations_export.csv): ").strip()
            export_to_csv(filename or "investigations_export.csv")
        
        elif choice == '9':
            print("\nEnter SQL query (or 'back' to return):")
            query = input("SQL> ").strip()
            
            if query.lower() != 'back':
                try:
                    conn = sqlite3.connect(DB_PATH)
                    cursor = conn.cursor()
                    cursor.execute(query)
                    
                    rows = cursor.fetchall()
                    for row in rows:
                        print(row)
                    
                    conn.close()
                except Exception as e:
                    print(f"❌ Error: {str(e)}")
        
        elif choice == '0':
            print("\n👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice")


# ============================================================================
# QUICK ACCESS FUNCTIONS
# ============================================================================

def quick_summary():
    """Quick summary without menu"""
    get_fraud_summary()
    print()
    get_today_stats()


if __name__ == "__main__":
    import os
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        print("Run batch_fraud_investigation.py first to create it")
    else:
        # Check if running with arguments
        import sys
        
        if len(sys.argv) > 1:
            if sys.argv[1] == '--summary':
                quick_summary()
            elif sys.argv[1] == '--export':
                export_to_csv()
            elif sys.argv[1] == '--today':
                get_today_stats()
            else:
                print("Usage:")
                print("  python explore_database.py           # Interactive menu")
                print("  python explore_database.py --summary # Quick summary")
                print("  python explore_database.py --export  # Export to CSV")
                print("  python explore_database.py --today   # Today's stats")
        else:
            main_menu()
