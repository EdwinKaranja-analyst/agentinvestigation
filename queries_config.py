"""
Query Registry - Simple configuration for all SQL queries

To add a new query:
1. Add entry to QUERIES dict
2. Create the SQL file
3. Done!

The engine will auto-execute based on allegation type.
"""

QUERIES = {
    'dfrs_signals': {
        'file': 'dfrs_signals.sql',
        'params': ['imei', 'account_number'],
        'run_for': ['hacking_tampering', 'hardware_theft'],
        'requires': 'SupportsDFRS',  # Only run if device supports DFRS
        'description': 'Device fraud risk signals and tampering scores'
    },
    
    'login_risk': {
        'file': 'login_risk_signals.sql',
        'params': ['customer_id'],
        'run_for': ['cash_loan_fraud', 'identity_theft', 'hacking_tampering'],
        'description': 'Login behavior and device sharing patterns'
    },
    
    'payment_match': {
        'file': 'payment_match.sql',
        'params': ['account_id'],
        'run_for': ['cash_payments', '3rd_party_cash'],
        'description': 'Payment name/phone matching and KYC risk'
    },
    
    'historical_tickets': {
        'file': 'historical_tickets.sql',
        'params': ['imei', 'account_number'],
        'run_for': ['all'],  # Always run (fast query)
        'description': 'Previous tickets for this account'
    }
}


def should_run_query(query_name, allegation, account_data):
    """
    Determine if a query should run based on allegation and conditions
    
    Args:
        query_name: Name of query in QUERIES dict
        allegation: Primary allegation (e.g., 'cash_loan_fraud')
        account_data: Account data dict (for checking conditions)
        
    Returns:
        Boolean - True if query should run
    """
    config = QUERIES.get(query_name)
    if not config:
        return False
    
    # Check if allegation matches
    run_for = config.get('run_for', [])
    if 'all' not in run_for and allegation not in run_for:
        return False
    
    # Check additional requirements
    requires = config.get('requires')
    if requires:
        if requires == 'SupportsDFRS':
            if not account_data or not account_data.get('SupportsDFRS'):
                return False
    
    return True


def get_query_params(query_name, identifiers, account_data):
    """
    Get parameters for a query from identifiers and account data
    
    Args:
        query_name: Name of query in QUERIES dict
        identifiers: Dict from query planning (imei, phone, account_number, etc.)
        account_data: Account data dict (CustomerId, AccountId, etc.)
        
    Returns:
        Tuple of parameter values
    """
    config = QUERIES.get(query_name)
    if not config:
        return ()
    
    param_names = config.get('params', [])
    param_values = []
    
    for param in param_names:
        # Try to get from identifiers first
        if param in identifiers:
            param_values.append(identifiers.get(param))
        # Then try account_data
        elif account_data:
            # Map parameter names to account data fields
            param_map = {
                'customer_id': 'CustomerId',
                'account_id': 'AccountId',
                'imei': 'IMEI',
                'account_number': 'AccountNumber'
            }
            field_name = param_map.get(param, param)
            param_values.append(account_data.get(field_name))
        else:
            param_values.append(None)
    
    return tuple(param_values)