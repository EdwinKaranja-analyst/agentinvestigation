"""
Simple batch investigation runner
"""

import sys
from engine import investigate_ticket

def run_batch(ticket_ids, use_cache=True):
    """
    Run investigations on multiple tickets
    
    Args:
        ticket_ids: List of ticket IDs
        use_cache: Use cached results
    """
    
    results = []
    
    for i, ticket_id in enumerate(ticket_ids, 1):
        print(f"\n{'='*70}")
        print(f"Processing {i}/{len(ticket_ids)}: Ticket #{ticket_id}")
        print(f"{'='*70}")
        
        try:
            result = investigate_ticket(ticket_id, use_cache=use_cache)
            results.append(result)
            
            status = result.get('fraud_status', 'Unknown')
            confidence = result.get('confidence', 0)
            print(f"\n✅ {status} ({confidence:.0%})")
            
        except Exception as e:
            print(f"\n❌ Failed: {e}")
            results.append({
                'ticket_id': ticket_id,
                'success': False,
                'error': str(e)
            })
    
    # Summary
    print(f"\n\n{'='*70}")
    print("BATCH SUMMARY")
    print(f"{'='*70}")
    print(f"Total: {len(results)}")
    print(f"Successful: {sum(1 for r in results if r.get('success'))}")
    print(f"Failed: {sum(1 for r in results if not r.get('success'))}")
    
    fraud_count = sum(1 for r in results if r.get('fraud_status') == 'Likely fraud')
    print(f"\nFraud detected: {fraud_count}")
    print(f"Not fraud: {len(results) - fraud_count}")
    
    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python batch_runner.py <ticket_id1> <ticket_id2> ...")
        print("  python batch_runner.py --file tickets.txt")
        sys.exit(1)
    
    # Read from file or args
    if sys.argv[1] == '--file':
        with open(sys.argv[2]) as f:
            ticket_ids = [line.strip() for line in f if line.strip()]
    else:
        ticket_ids = sys.argv[1:]
    
    run_batch(ticket_ids)