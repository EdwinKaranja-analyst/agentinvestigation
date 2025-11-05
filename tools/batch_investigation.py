#!/usr/bin/env python3
"""
M-KOPA BATCH FRAUD INVESTIGATION SYSTEM
Processes multiple tickets with intelligent caching

Features:
- Fetch tickets from Freshservice OR read from file
- SQLite cache - skip already investigated tickets
- Force re-run option
- Stop on first error
- Simple progress counter
- Individual JSON files + master index
- Summary report

Usage:
    # Fetch from Freshservice automatically
    python batch_investigation.py --fetch-open
    
    # Use ticket list from file
    python batch_investigation.py tickets.txt
    
    # Force re-investigation of all tickets
    python batch_investigation.py tickets.txt --force

Author: M-KOPA Fraud Team
Date: October 2025
"""

import os
import sys
import json
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests

# Import the core investigation system
try:
    from core.engine import run_dynamic_investigation
    SYSTEM_AVAILABLE = True
except ImportError:
    print("❌ Could not import COMPLETE_DYNAMIC_FRAUD_SYSTEM_FIXED.py")
    print("Make sure the file is in the same directory")
    SYSTEM_AVAILABLE = False


# ============================================================================
# CONFIGURATION
# ============================================================================

FRESHSERVICE_BASE_URL = "https://m-kopaservicedesk.freshservice.com/api/v2"
CACHE_DB_PATH = "investigations_cache.db"
RESULTS_DIR = "batch_results"


# ============================================================================
# CACHE DATABASE (SQLite)
# ============================================================================

def init_cache_database():
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
    
    # Create index for faster lookups
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
    
    print(f"✅ Cache database initialized: {CACHE_DB_PATH}")
    print(f"   Storage: Database only (no JSON files)")



def check_if_investigated(ticket_id: str) -> Optional[Dict[str, Any]]:
    """
    Check if ticket was already investigated
    
    Returns: Investigation record if found, None otherwise
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


def save_to_cache(ticket_id: str, result: Dict[str, Any], result_file: Optional[str] = None):
    """
    Save investigation result to cache database
    Now stores FULL investigation data in database (no JSON files needed)
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
# FRESHSERVICE TICKET FETCHING
# ============================================================================

def fetch_open_fraud_tickets() -> List[str]:
    """
    Fetch all open fraud tickets from Freshservice
    
    Returns: List of ticket IDs
    """
    
    API_KEY = os.getenv('FRESHSERVICE_API_KEY')
    
    if not API_KEY:
        raise ValueError("FRESHSERVICE_API_KEY environment variable not set")
    
    print("🔍 Fetching open fraud tickets from Freshservice...")
    
    try:
        # Fetch tickets assigned to Fraud Team that are Open or Pending
        params = {
            'query': '"group_id:27000198468 AND (status:2 OR status:3)"'
            # 27000198468 = Fraud Team group ID from your config
            # status:2 = Open, status:3 = Pending
        }
        
        response = requests.get(
            f"{FRESHSERVICE_BASE_URL}/tickets",
            auth=(API_KEY, 'X'),
            params=params,
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"Freshservice API failed: {response.status_code} - {response.text}")
        
        data = response.json()
        tickets = data.get('tickets', [])
        
        ticket_ids = [str(ticket['id']) for ticket in tickets]
        
        print(f"✅ Found {len(ticket_ids)} open fraud tickets")
        
        # Show sample
        for ticket in tickets[:5]:
            print(f"   #{ticket['id']}: {ticket.get('subject', 'No subject')[:60]}")
        
        if len(tickets) > 5:
            print(f"   ... and {len(tickets) - 5} more tickets")
        
        return ticket_ids
        
    except Exception as e:
        print(f"❌ Failed to fetch tickets: {str(e)}")
        raise


# ============================================================================
# BATCH PROCESSING
# ============================================================================

def process_batch(
    ticket_ids: List[str],
    use_analytics: bool = True,
    dry_run: bool = True,
    force_rerun: bool = False,
    stop_on_error: bool = True
) -> Dict[str, Any]:
    """
    Process a batch of tickets with caching
    
    Args:
        ticket_ids: List of ticket IDs to investigate
        use_analytics: Enable Azure SQL queries
        dry_run: Preview mode (don't update Freshservice)
        force_rerun: Re-investigate even if cached
        stop_on_error: Stop batch on first error
    
    Returns:
        Batch results summary
    """
    
    print("\n" + "="*70)
    print("🚀 BATCH FRAUD INVESTIGATION - DATABASE ONLY")
    print("="*70)
    print(f"Tickets to process: {len(ticket_ids)}")
    print(f"Use analytics: {use_analytics}")
    print(f"Dry run: {dry_run}")
    print(f"Force re-run: {force_rerun}")
    print(f"Stop on error: {stop_on_error}")
    print(f"Storage: SQLite database only (no JSON files)")
    print("="*70 + "\n")
    
    # Initialize tracking
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    results = {
        'batch_id': timestamp,
        'start_time': datetime.utcnow().isoformat(),
        'configuration': {
            'use_analytics': use_analytics,
            'dry_run': dry_run,
            'force_rerun': force_rerun,
            'stop_on_error': stop_on_error
        },
        'tickets': {
            'total': len(ticket_ids),
            'investigated': 0,
            'cached': 0,
            'failed': 0
        },
        'investigations': [],
        'errors': [],
        'summary': {}
    }
    
    # Process each ticket
    for idx, ticket_id in enumerate(ticket_ids, 1):
        print(f"\n{'='*70}")
        print(f"📋 Processing ticket {idx}/{len(ticket_ids)}: #{ticket_id}")
        print(f"{'='*70}")
        
        try:
            # Check cache
            if not force_rerun:
                cached = check_if_investigated(ticket_id)
                
                if cached:
                    print(f"⏭️  CACHED - Already investigated on {cached['investigation_date']}")
                    print(f"   Status: {cached['fraud_status']}")
                    print(f"   Confidence: {cached['confidence']:.0%}")
                    print(f"   Result file: {cached['result_file']}")
                    print(f"   Skipping...")
                    
                    results['tickets']['cached'] += 1
                    results['investigations'].append({
                        'ticket_id': ticket_id,
                        'status': 'cached',
                        'cached_data': cached
                    })
                    continue
            
            # Run investigation
            print(f"🔍 Running investigation...")
            
            investigation_result = run_dynamic_investigation(
                ticket_id=ticket_id,
                use_analytics=use_analytics,
                dry_run=dry_run
            )
            
            # Save to database ONLY (no JSON files)
            save_to_cache(ticket_id, investigation_result, None)
            
            # Extract summary info
            phase2_data = investigation_result.get('phases', {}).get('phase2', {}).get('data', {})
            phase4_data = investigation_result.get('phases', {}).get('phase4', {}).get('data', {})
            
            fraud_status = phase4_data.get('fraud_status', 'Unknown')
            confidence = phase4_data.get('confidence', 0.0)
            fraud_type = phase2_data.get('fraud_type', 'Unknown')
            
            print(f"\n✅ INVESTIGATION COMPLETE")
            print(f"   Classification: {fraud_status}")
            print(f"   Confidence: {confidence:.0%}")
            print(f"   Type: {fraud_type}")
            print(f"   Saved to: Database")
            
            results['tickets']['investigated'] += 1
            results['investigations'].append({
                'ticket_id': ticket_id,
                'status': 'success',
                'fraud_status': fraud_status,
                'confidence': confidence,
                'fraud_type': fraud_type
            })
            
        except Exception as e:
            print(f"\n❌ INVESTIGATION FAILED: {str(e)}")
            
            results['tickets']['failed'] += 1
            results['errors'].append({
                'ticket_id': ticket_id,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
            
            if stop_on_error:
                print(f"\n🛑 Stopping batch - error on ticket #{ticket_id}")
                print(f"   Configure with --continue-on-error to skip failed tickets")
                break
            else:
                print(f"⚠️  Continuing to next ticket...")
                continue
    
    # ========================================================================
    # GENERATE SUMMARY
    # ========================================================================
    
    results['end_time'] = datetime.utcnow().isoformat()
    
    # Calculate statistics
    investigated = [inv for inv in results['investigations'] if inv['status'] == 'success']
    
    fraud_detected = len([inv for inv in investigated if inv.get('fraud_status') == 'Likely fraud'])
    not_fraud = len([inv for inv in investigated if inv.get('fraud_status') == 'Not fraud'])
    
    if investigated:
        avg_confidence = sum(inv.get('confidence', 0) for inv in investigated) / len(investigated)
    else:
        avg_confidence = 0.0
    
    results['summary'] = {
        'total_tickets': len(ticket_ids),
        'investigated': results['tickets']['investigated'],
        'cached': results['tickets']['cached'],
        'failed': results['tickets']['failed'],
        'fraud_detected': fraud_detected,
        'not_fraud': not_fraud,
        'fraud_rate': (fraud_detected / len(investigated) * 100) if investigated else 0,
        'average_confidence': avg_confidence
    }
    
    # Save master results to database (summary record)
    print(f"\n💾 All results saved to database: {CACHE_DB_PATH}")
    
    # Print summary
    print("\n" + "="*70)
    print("📊 BATCH INVESTIGATION SUMMARY")
    print("="*70)
    print(f"\n📋 Tickets Processed:")
    print(f"   Total: {results['tickets']['total']}")
    print(f"   Investigated: {results['tickets']['investigated']}")
    print(f"   Cached (skipped): {results['tickets']['cached']}")
    print(f"   Failed: {results['tickets']['failed']}")
    
    print(f"\n🔍 Investigation Results:")
    print(f"   Fraud detected: {fraud_detected} ({fraud_detected/len(investigated)*100:.1f}%)" if investigated else "   No investigations completed")
    print(f"   Not fraud: {not_fraud} ({not_fraud/len(investigated)*100:.1f}%)" if investigated else "")
    print(f"   Average confidence: {avg_confidence:.0%}" if investigated else "")
    
    if results['errors']:
        print(f"\n⚠️  Errors encountered: {len(results['errors'])}")
        for error in results['errors'][:3]:
            print(f"   - Ticket #{error['ticket_id']}: {error['error'][:60]}")
    
    print(f"\n💾 Results stored in:")
    print(f"   Database: {CACHE_DB_PATH}")
    print(f"   Query with: sqlite3 {CACHE_DB_PATH}")
    print(f"   Or use Python to explore the data")
    
    # Get overall cache stats
    cache_stats = get_cache_stats()
    print(f"\n📊 Cache Statistics (All Time):")
    print(f"   Total investigations: {cache_stats['total_investigations']}")
    print(f"   Fraud detected: {cache_stats['fraud_detected']}")
    print(f"   Not fraud: {cache_stats['not_fraud']}")
    
    print("\n" + "="*70)
    
    return results


# ============================================================================
# TICKET LOADING
# ============================================================================

def load_tickets_from_file(filepath: str) -> List[str]:
    """Load ticket IDs from text file (one per line)"""
    
    print(f"📄 Loading tickets from: {filepath}")
    
    try:
        with open(filepath, 'r') as f:
            # Read lines, strip whitespace, ignore empty lines and comments
            ticket_ids = [
                line.strip() 
                for line in f 
                if line.strip() and not line.strip().startswith('#')
            ]
        
        print(f"✅ Loaded {len(ticket_ids)} tickets from file")
        return ticket_ids
        
    except FileNotFoundError:
        print(f"❌ File not found: {filepath}")
        raise
    except Exception as e:
        print(f"❌ Error reading file: {str(e)}")
        raise


# ============================================================================
# MAIN PROGRAM
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='M-KOPA Batch Fraud Investigation System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch open tickets from Freshservice
  python batch_investigation.py --fetch-open
  
  # Use ticket list from file
  python batch_investigation.py tickets.txt
  
  # Force re-investigation (ignore cache)
  python batch_investigation.py tickets.txt --force
  
  # Continue on errors instead of stopping
  python batch_investigation.py tickets.txt --continue-on-error
  
  # Disable analytics (faster, no DB)
  python batch_investigation.py tickets.txt --no-analytics
  
  # Live mode (actually update Freshservice)
  python batch_investigation.py tickets.txt --live
        """
    )
    
    # Input source
    parser.add_argument(
        'ticket_file',
        nargs='?',
        help='Path to file containing ticket IDs (one per line)'
    )
    
    parser.add_argument(
        '--fetch-open',
        action='store_true',
        help='Fetch open tickets from Freshservice automatically'
    )
    
    # Options
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-investigation (ignore cache)'
    )
    
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue batch if a ticket fails (default: stop on error)'
    )
    
    parser.add_argument(
        '--no-analytics',
        action='store_true',
        help='Disable Azure SQL queries (Phase 3)'
    )
    
    parser.add_argument(
        '--live',
        action='store_true',
        help='Live mode - actually update Freshservice (default: dry run)'
    )
    
    parser.add_argument(
        '--clear-cache',
        action='store_true',
        help='Clear cache database before running'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.fetch_open and not args.ticket_file:
        parser.error("Provide either --fetch-open or a ticket file")
    
    if args.fetch_open and args.ticket_file:
        parser.error("Cannot use both --fetch-open and ticket file")
    
    if not SYSTEM_AVAILABLE:
        print("❌ Core investigation system not available")
        sys.exit(1)
    
    # Check API keys
    if not os.getenv('ANTHROPIC_API_KEY'):
        print("❌ ANTHROPIC_API_KEY not set")
        print("Set it with: export ANTHROPIC_API_KEY='your_key'")
        sys.exit(1)
    
    if args.fetch_open and not os.getenv('FRESHSERVICE_API_KEY'):
        print("❌ FRESHSERVICE_API_KEY not set (required for --fetch-open)")
        print("Set it with: export FRESHSERVICE_API_KEY='your_key'")
        sys.exit(1)
    
    # Initialize cache
    if args.clear_cache:
        if os.path.exists(CACHE_DB_PATH):
            os.remove(CACHE_DB_PATH)
            print(f"🗑️  Cache cleared: {CACHE_DB_PATH}")
    
    init_cache_database()
    
    # Get ticket list
    if args.fetch_open:
        print("\n📥 Fetching tickets from Freshservice...")
        ticket_ids = fetch_open_fraud_tickets()
    else:
        ticket_ids = load_tickets_from_file(args.ticket_file)
    
    if not ticket_ids:
        print("❌ No tickets to process")
        sys.exit(1)
    
    # Confirm before proceeding
    print(f"\n⚠️  About to process {len(ticket_ids)} tickets")
    
    if not args.live:
        print("   Mode: DRY RUN (preview only)")
    else:
        print("   Mode: LIVE (will update Freshservice)")
        response = input("\n   Are you sure? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Cancelled by user")
            sys.exit(0)
    
    # Process batch
    results = process_batch(
        ticket_ids=ticket_ids,
        use_analytics=not args.no_analytics,
        dry_run=not args.live,
        force_rerun=args.force,
        stop_on_error=not args.continue_on_error
    )
    
    print("\n✅ Batch processing complete!")
    
    return results


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
