"""M-KOPA Fraud Investigation - Configuration Package"""
from .settings import *
from .queries import SQL_QUERIES
from .instructions import (
    get_query_planning_prompt, 
    get_investigation_prompt, 
    TRAINING_EXAMPLES
)