"""
Batch investigation runner with optional Freshservice updates
"""

import sys
import argparse
import os
from engine import investigate_ticket

# //TODO: Make this more clearer. Especially for when I want to update
# //TODO: Segment the update into FS into just updating and resolving



def select_file_interactive():
    """
    Interactive file selector - shows available files and lets user choose
    
    Returns:
        str: Selected file path or None if cancelled
    """
    
    # Look for files in investigated folder
    investigated_dir = 'investigated'
    
    if not os.path.exists(investigated_dir):
        print(f"\n❌ Error: '{investigated_dir}/' folder not found!")
        print(f"   Please create the folder and add ticket files:")
        print(f"   mkdir {investigated_dir}")
        return None
    
    # Get all .txt files in investigated folder
    try:
        all_files = os.listdir(investigated_dir)
        txt_files = [f for f in all_files if f.endswith('.txt')]
    except Exception as e:
        print(f"\n❌ Error reading '{investigated_dir}/' folder: {e}")
        return None
    
    if not txt_files:
        print(f"\n❌ No .txt files found in '{investigated_dir}/' folder!")
        print(f"   Please add ticket files (one ticket ID per line)")
        print(f"\n   Example: {investigated_dir}/tickets.txt")
        return None
    
    # Count tickets in each file
    file_info = []
    for filename in sorted(txt_files):
        filepath = os.path.join(investigated_dir, filename)
        try:
            with open(filepath) as f:
                ticket_count = sum(1 for line in f if line.strip() and not line.strip().startswith('#'))
            file_info.append((filename, ticket_count, filepath))
        except Exception as e:
            file_info.append((filename, f"Error: {e}", filepath))
    
    # Display menu
    print(f"\n{'='*70}")
    print(f"📁 Available ticket files in '{investigated_dir}/':")
    print(f"{'='*70}\n")
    
    for i, (filename, count, _) in enumerate(file_info, 1):
        if isinstance(count, int):
            print(f"  {i}. {filename:<40} ({count} tickets)")
        else:
            print(f"  {i}. {filename:<40} ({count})")
    
    print(f"\n  0. Cancel")
    print(f"{'='*70}")
    
    # Get user selection
    while True:
        try:
            choice = input("\nSelect file number (0 to cancel): ").strip()
            
            if choice == '0':
                print("\n❌ Cancelled")
                return None
            
            choice_num = int(choice)
            
            if 1 <= choice_num <= len(file_info):
                selected_file = file_info[choice_num - 1]
                filename, count, filepath = selected_file
                
                print(f"\n✅ Selected: {filename}")
                if isinstance(count, int):
                    print(f"✅ Contains: {count} tickets")
                
                return filepath
            else:
                print(f"❌ Invalid choice. Please enter 0-{len(file_info)}")
                
        except ValueError:
            print("❌ Please enter a number")
        except KeyboardInterrupt:
            print("\n\n❌ Cancelled")
            return None


def run_batch(ticket_ids, use_cache=True, update_freshservice=False):
    """
    Run investigations on multiple tickets
    
    Args:
        ticket_ids: List of ticket IDs
        use_cache: Use cached results
        update_freshservice: Update Freshservice tickets with priority/comments
    """
    
    import time
    batch_start_time = time.time()
    
    results = []
    
    print(f"\n{'='*70}")
    print(f"BATCH INVESTIGATION")
    print(f"{'='*70}")
    print(f"Tickets: {len(ticket_ids)}")
    print(f"Cache: {'Enabled' if use_cache else 'Disabled'}")
    print(f"Freshservice Updates: {'Enabled' if update_freshservice else 'Disabled'}")
    print(f"{'='*70}\n")
    
    for i, ticket_id in enumerate(ticket_ids, 1):
        print(f"\n{'='*70}")
        print(f"Processing {i}/{len(ticket_ids)}: Ticket #{ticket_id}")
        print(f"{'='*70}")
        
        try:
            result = investigate_ticket(
                ticket_id, 
                use_cache=use_cache,
                update_freshservice=update_freshservice
            )
            results.append(result)
            
            # Display result
            if result.get('wrong_escalation'):
                print(f"\n⚠️  Wrong Escalation")
            else:
                status = result.get('fraud_status', 'Unknown')
                confidence = result.get('confidence', 0)
                priority = result.get('priority_label', 'N/A')
                print(f"\n✅ {status} ({confidence:.0%}) - Priority: {priority}")
            
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
    
    # Wrong escalations
    wrong_escalations = sum(1 for r in results if r.get('wrong_escalation'))
    if wrong_escalations:
        print(f"\n⚠️  Wrong Escalations: {wrong_escalations}")
    
    # Fraud breakdown
    successful_investigations = [r for r in results if r.get('success') and not r.get('wrong_escalation')]
    if successful_investigations:
        fraud_count = sum(1 for r in successful_investigations if r.get('fraud_status') == 'Likely fraud')
        confirmed_count = sum(1 for r in successful_investigations if r.get('fraud_status') == 'Confirmed fraud')
        not_fraud_count = sum(1 for r in successful_investigations if r.get('fraud_status') == 'Not fraud')
        
        print(f"\nFRAUD CLASSIFICATION:")
        print(f"  Confirmed fraud: {confirmed_count}")
        print(f"  Likely fraud: {fraud_count}")
        print(f"  Not fraud: {not_fraud_count}")
    
    # Priority breakdown
    if successful_investigations:
        critical = sum(1 for r in successful_investigations if r.get('priority') == 4)
        high = sum(1 for r in successful_investigations if r.get('priority') == 3)
        medium = sum(1 for r in successful_investigations if r.get('priority') == 2)
        low = sum(1 for r in successful_investigations if r.get('priority') == 1)
        
        print(f"\nPRIORITY BREAKDOWN:")
        print(f"  🔴 Critical (4): {critical}")
        print(f"  🟠 High (3): {high}")
        print(f"  🟡 Medium (2): {medium}")
        print(f"  🟢 Low (1): {low}")
    
    # Freshservice updates
    if update_freshservice:
        updated_count = sum(1 for r in results if r.get('freshservice_updated'))
        print(f"\nFreshservice Updates: {updated_count}/{len(results)}")
    
    # Timing summary
    batch_elapsed_time = time.time() - batch_start_time
    print(f"\n⏱️  TIMING:")
    print(f"  Total time: {batch_elapsed_time:.2f} seconds")
    print(f"  Average per ticket: {batch_elapsed_time/len(results):.2f} seconds")
    
    # Individual timings (if available)
    timings = [r.get('elapsed_time') for r in results if r.get('elapsed_time')]
    if timings:
        print(f"  Fastest: {min(timings):.2f}s | Slowest: {max(timings):.2f}s")
    
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run fraud investigations on multiple tickets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Investigate tickets (no Freshservice update)
  python batch_runner.py 151333 151334 151335
  
  # Investigate and update Freshservice
  python batch_runner.py --update 151333 151334 151335
  
  # Read from file and update Freshservice
  python batch_runner.py --update --file tickets.txt
  
  # Disable cache
  python batch_runner.py --no-cache --update 151333 151334
        """
    )
    
    parser.add_argument(
        'ticket_ids',
        nargs='*',
        help='Ticket IDs to investigate'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        nargs='?',
        const='SELECT',
        help='Read ticket IDs from file (one per line). Use --file without filename for interactive selection.'
    )
    
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update Freshservice tickets with priority and comments (default: False)'
    )
    
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Disable cache (force fresh investigation)'
    )
    
    args = parser.parse_args()
    
    # Get ticket IDs
    if args.file:
        # Check if user wants interactive selection
        if args.file == 'SELECT':
            print("\n🎯 Interactive File Selection Mode")
            file_path = select_file_interactive()
            
            if not file_path:
                sys.exit(1)
        else:
            # User provided filename - find it
            import os
            possible_paths = [
                os.path.join('investigated', args.file),  # investigated/tickets.txt
                args.file  # tickets.txt (current directory)
            ]
            
            file_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    file_path = path
                    break
            
            if not file_path:
                print(f"❌ Error: File not found!")
                print(f"   Looked in:")
                for path in possible_paths:
                    print(f"   - {path}")
                print(f"\n   Tip: Use --file without filename for interactive selection")
                sys.exit(1)
            
            print(f"📄 Selected file: {file_path}")
        
        # Read tickets from selected file
        print(f"\n📄 Reading tickets from: {file_path}")
        
        with open(file_path) as f:
            ticket_ids = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        
        if not ticket_ids:
            print(f"❌ Error: No ticket IDs found in {file_path}")
            print(f"   File should contain one ticket ID per line")
            sys.exit(1)
        
        print(f"✅ Loaded {len(ticket_ids)} ticket IDs\n")
        
    elif args.ticket_ids:
        ticket_ids = args.ticket_ids
    else:
        parser.print_help()
        sys.exit(1)
    
    # Run batch
    run_batch(
        ticket_ids,
        use_cache=not args.no_cache,
        update_freshservice=args.update
    )