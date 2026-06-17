"""Spillway error-column names — shared by Channel and Assert modules.

Every DataFrame routed through a spillway edge carries these columns so
``error_types`` filter-matching on the receiving module works consistently.
"""

AQ_ERROR_MODULE = "_aq_error_module"
AQ_ERROR_TYPE   = "_aq_error_type"
AQ_ERROR_MSG    = "_aq_error_msg"
AQ_ERROR_TS     = "_aq_error_ts"
AQ_ERROR_RULE   = "_aq_error_rule"
