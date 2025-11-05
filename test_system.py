#!/usr/bin/env python3
"""
Quick test script for modular fraud investigation system
Run this to verify everything works
"""

import sys
import os

print("🧪 TESTING MODULAR FRAUD INVESTIGATION SYSTEM")
print("="*70)

# Test 1: Imports
print("\n1️⃣ Testing imports...")
try:
    from core.engine import run_investigation
    print("   ✅ core.engine")
    
    from config.settings import THRESHOLDS, CLAUDE_MODEL
    print("   ✅ config.settings")
    
    from config.queries import SQL_QUERIES
    print(f"   ✅ config.queries ({len(SQL_QUERIES)} queries loaded)")
    
    from config.instructions import TRAINING_EXAMPLES
    print(f"   ✅ config.instructions ({len(TRAINING_EXAMPLES)} examples loaded)")
    
    from core.database import init_database
    print("   ✅ core.database")
    
    from core.api import fetch_freshservice_ticket
    print("   ✅ core.api")
    
except ImportError as e:
    print(f"   ❌ Import failed: {e}")
    sys.exit(1)

# Test 2: Database
print("\n2️⃣ Testing database...")
try:
    db_path = init_database()
    print(f"   ✅ Database initialized: {db_path}")
except Exception as e:
    print(f"   ❌ Database failed: {e}")
    sys.exit(1)

# Test 3: Check environment
print("\n3️⃣ Checking environment...")
if os.getenv('ANTHROPIC_API_KEY'):
    print("   ✅ ANTHROPIC_API_KEY is set")
else:
    print("   ⚠️  ANTHROPIC_API_KEY not set (required for investigations)")

if os.getenv('FRESHSERVICE_API_KEY'):
    print("   ✅ FRESHSERVICE_API_KEY is set")
else:
    print("   ⚠️  FRESHSERVICE_API_KEY not set (required for Phase 1)")

# Test 4: Config values
print("\n4️⃣ Checking config values...")
print(f"   Claude Model: {CLAUDE_MODEL}")
print(f"   Fraud Score Threshold: {THRESHOLDS['fraud_score_critical']}")
print(f"   Tamper Score Threshold: {THRESHOLDS['tamper_score_critical']}")
print(f"   SQL Queries available: {', '.join(SQL_QUERIES.keys())}")

print("\n" + "="*70)
print("🎉 BASIC TESTS PASSED!")
print("="*70)

print("\nSystem is ready for investigation testing!")
print("\nNext steps:")
print("  1. Set ANTHROPIC_API_KEY if not set")
print("  2. Set FRESHSERVICE_API_KEY if not set")
print("  3. Run: python tools/batch_investigation.py tickets.txt")
print("  4. Or: streamlit run tools/streamlit_app.py")

