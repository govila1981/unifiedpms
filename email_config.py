"""
Email Configuration Module
Stores email settings and templates for SendGrid
"""

import os
from typing import Dict, List, Optional
from dataclasses import dataclass

# Try to import streamlit for secrets management
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False


@dataclass
class EmailConfig:
    """Email configuration settings"""
    sendgrid_api_key: str
    from_email: str
    from_name: str = "Trade Processing System"

    @classmethod
    def from_streamlit_secrets(cls):
        """Load configuration from Streamlit secrets (with environment variable fallback)"""
        # Priority 1: Try environment variables first (for Railway/production)
        env_config = cls.from_env()
        if env_config.is_configured():
            return env_config

        # Priority 2: Try Streamlit secrets (for local development)
        if not STREAMLIT_AVAILABLE:
            return cls(sendgrid_api_key='', from_email='', from_name='')

        try:
            # Access secrets from st.secrets
            api_key = st.secrets.get('SENDGRID_API_KEY', '')
            from_email = st.secrets.get('SENDGRID_FROM_EMAIL', '')
            from_name = st.secrets.get('SENDGRID_FROM_NAME', 'Aurigin Trade Processing')

            return cls(
                sendgrid_api_key=api_key,
                from_email=from_email,
                from_name=from_name
            )
        except Exception as e:
            # If secrets not configured, return empty config
            return cls(sendgrid_api_key='', from_email='', from_name='')

    @classmethod
    def from_env(cls):
        """Load configuration from environment variables (fallback)"""
        api_key = os.getenv('SENDGRID_API_KEY', '')
        from_email = os.getenv('SENDGRID_FROM_EMAIL', '')
        from_name = os.getenv('SENDGRID_FROM_NAME', 'Trade Processing System')

        return cls(
            sendgrid_api_key=api_key,
            from_email=from_email,
            from_name=from_name
        )

    def is_configured(self) -> bool:
        """Check if email is properly configured"""
        return bool(self.sendgrid_api_key and self.from_email)


# Email templates
EMAIL_TEMPLATES = {
    'stage1_complete': {
        'subject': '{fund_name} | Trade Processing | {date}',
        'body': '''
<h2>Trade Processing Completed Successfully</h2>

<p>The trade processing pipeline has completed successfully.</p>

<h3>Summary:</h3>
<ul>
    <li><strong>Account:</strong> {account_prefix}</li>
    <li><strong>Timestamp:</strong> {timestamp}</li>
    <li><strong>Total Trades Processed:</strong> {total_trades}</li>
    <li><strong>Starting Positions:</strong> {starting_positions}</li>
    <li><strong>Final Positions:</strong> {final_positions}</li>
</ul>

<h3>Attached Files:</h3>
<ul>
    {file_list}
</ul>

<p>Please review the attached reports and verify all positions.</p>

<hr>
<p style="color: #666; font-size: 12px;">
This is an automated email from the Trade Processing System.<br>
Generated on {generated_time}
</p>
'''
    },

    'acm_complete': {
        'subject': '{fund_name} | ACM Export | {date}',
        'body': '''
<h2>ACM Export Completed Successfully</h2>

<p>The ACM mapping has been completed and is ready for upload.</p>

<h3>Summary:</h3>
<ul>
    <li><strong>Account:</strong> {account_prefix}</li>
    <li><strong>Timestamp:</strong> {timestamp}</li>
    <li><strong>Total Records:</strong> {total_records}</li>
    <li><strong>Errors:</strong> {error_count}</li>
</ul>

<h3>Attached Files:</h3>
<ul>
    <li>ACM CSV Output</li>
    <li>Error Report (if any)</li>
    <li>Schema Reference</li>
</ul>

<p>The ACM file is ready for upload to the system.</p>

<hr>
<p style="color: #666; font-size: 12px;">
This is an automated email from the Trade Processing System.<br>
Generated on {generated_time}
</p>
'''
    },

    'deliverables_complete': {
        'subject': '{fund_name} | Deliverables Report | {date}',
        'body': '''
<h2>Physical Deliverables Report</h2>

<p>The physical deliverables calculation has been completed.</p>

<h3>Summary:</h3>
<ul>
    <li><strong>Account:</strong> {account_prefix}</li>
    <li><strong>Timestamp:</strong> {timestamp}</li>
    <li><strong>Total Underlyings:</strong> {total_underlyings}</li>
    <li><strong>Total Net Deliverables:</strong> {total_deliverables}</li>
</ul>

<h3>Attached Files:</h3>
<ul>
    <li>Deliverables Report (Excel with formulas)</li>
</ul>

<p>Please review the deliverables and prepare for physical settlement.</p>

<hr>
<p style="color: #666; font-size: 12px;">
This is an automated email from the Trade Processing System.<br>
Generated on {generated_time}
</p>
'''
    },

    'expiry_delivery': {
        'subject': '{fund_name} | Expiry Delivery | {expiry_date}',
        'body': '''
<h2>Expiry Physical Delivery Report</h2>

<p>Physical delivery report for expiring positions.</p>

<h3>Expiry Details:</h3>
<ul>
    <li><strong>Expiry Date:</strong> {expiry_date}</li>
    <li><strong>Account:</strong> {account_prefix}</li>
    <li><strong>Total Positions:</strong> {total_positions}</li>
    <li><strong>Cash Settlements:</strong> {cash_settlements}</li>
</ul>

<h3>Attached Files:</h3>
<ul>
    <li>Expiry Delivery Excel</li>
    <li>ACM Format Output</li>
</ul>

<p><strong style="color: red;">Action Required:</strong> Review and process physical deliveries before market close.</p>

<hr>
<p style="color: #666; font-size: 12px;">
This is an automated email from the Trade Processing System.<br>
Generated on {generated_time}
</p>
'''
    },

    'broker_recon': {
        'subject': '{fund_name} | Broker Reconciliation | {date}',
        'body': '''
<h2>Broker Reconciliation Report</h2>

<p>The broker reconciliation has been completed.</p>

<h3>Summary:</h3>
<ul>
    <li><strong>Timestamp:</strong> {timestamp}</li>
    <li><strong>Matched Trades:</strong> {matched_count}</li>
    <li><strong>Unmatched Clearing:</strong> {unmatched_clearing}</li>
    <li><strong>Unmatched Broker:</strong> {unmatched_broker}</li>
    <li><strong>Match Rate:</strong> {match_rate}%</li>
</ul>

<h3>Attached Files:</h3>
<ul>
    <li>Reconciliation Report (4 sheets)</li>
    <li>Enhanced Clearing File</li>
</ul>

<p>Please review unmatched items and resolve discrepancies.</p>

<hr>
<p style="color: #666; font-size: 12px;">
This is an automated email from the Trade Processing System.<br>
Generated on {generated_time}
</p>
'''
    },

    'error_notification': {
        'subject': '{fund_name} | ERROR - {error_type} | {date}',
        'body': '''
<h2 style="color: red;">Processing Error Occurred</h2>

<p>An error occurred during processing:</p>

<h3>Error Details:</h3>
<ul>
    <li><strong>Error Type:</strong> {error_type}</li>
    <li><strong>Timestamp:</strong> {timestamp}</li>
    <li><strong>Message:</strong> {error_message}</li>
</ul>

<h3>Error Trace:</h3>
<pre style="background: #f5f5f5; padding: 10px; border-radius: 5px;">
{error_trace}
</pre>

<p><strong>Action Required:</strong> Please check the system and resolve the error.</p>

<hr>
<p style="color: #666; font-size: 12px;">
This is an automated email from the Trade Processing System.<br>
Generated on {generated_time}
</p>
'''
    },

    'custom': {
        'subject': '{subject}',
        'body': '{body}'
    }
}


def get_default_recipients() -> List[str]:
    """Get default recipient list - always includes operations@aurigincm.com"""
    default_ops_email = 'operations@aurigincm.com'

    # Get additional recipients from environment
    additional = os.getenv('EMAIL_RECIPIENTS', '')
    recipients = [default_ops_email]

    if additional:
        for email in additional.split(','):
            email = email.strip()
            if email and email != default_ops_email:
                recipients.append(email)

    return recipients
