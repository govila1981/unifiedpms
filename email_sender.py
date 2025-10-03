"""
Email Sender Module using SendGrid
Handles sending emails with attachments for trade processing reports
"""

import base64
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId
)
from email_config import EmailConfig, EMAIL_TEMPLATES

logger = logging.getLogger(__name__)


class EmailSender:
    """Email sender using SendGrid API"""

    def __init__(self, config: Optional[EmailConfig] = None):
        """
        Initialize email sender

        Args:
            config: EmailConfig object. If None, loads from environment
        """
        self.config = config or EmailConfig.from_env()

        if not self.config.is_configured():
            logger.warning("Email not configured. Set SENDGRID_API_KEY and SENDGRID_FROM_EMAIL environment variables.")
            self.client = None
        else:
            self.client = SendGridAPIClient(api_key=self.config.sendgrid_api_key)

    def is_enabled(self) -> bool:
        """Check if email sending is enabled"""
        return self.client is not None

    def send_email(self,
                   to_emails: List[str],
                   subject: str,
                   html_body: str,
                   attachments: Optional[List[Path]] = None,
                   cc_emails: Optional[List[str]] = None,
                   bcc_emails: Optional[List[str]] = None) -> bool:
        """
        Send email with optional attachments

        Args:
            to_emails: List of recipient email addresses
            subject: Email subject
            html_body: HTML email body
            attachments: List of file paths to attach
            cc_emails: List of CC email addresses
            bcc_emails: List of BCC email addresses

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled():
            logger.warning("Email sending is disabled - not configured")
            return False

        try:
            # Create message
            message = Mail(
                from_email=(self.config.from_email, self.config.from_name),
                to_emails=to_emails,
                subject=subject,
                html_content=html_body
            )

            # Add CC recipients
            if cc_emails:
                for cc in cc_emails:
                    message.add_cc(cc)

            # Add BCC recipients
            if bcc_emails:
                for bcc in bcc_emails:
                    message.add_bcc(bcc)

            # Add attachments
            if attachments:
                for file_path in attachments:
                    if not Path(file_path).exists():
                        logger.warning(f"Attachment not found: {file_path}")
                        continue

                    try:
                        with open(file_path, 'rb') as f:
                            data = f.read()
                            encoded = base64.b64encode(data).decode()

                        attachment = Attachment(
                            FileContent(encoded),
                            FileName(Path(file_path).name),
                            FileType(self._get_mime_type(file_path)),
                            Disposition('attachment')
                        )
                        message.add_attachment(attachment)
                        logger.info(f"Added attachment: {Path(file_path).name}")
                    except Exception as e:
                        logger.error(f"Error attaching file {file_path}: {e}")

            # Send email
            response = self.client.send(message)

            if response.status_code in [200, 201, 202]:
                logger.info(f"Email sent successfully to {', '.join(to_emails)}")
                logger.info(f"SendGrid Response: {response.status_code}")
                return True
            else:
                logger.error(f"Email send failed: {response.status_code} - {response.body}")
                return False

        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False

    def send_from_template(self,
                          template_name: str,
                          to_emails: List[str],
                          template_data: Dict,
                          attachments: Optional[List[Path]] = None,
                          cc_emails: Optional[List[str]] = None) -> bool:
        """
        Send email using predefined template

        Args:
            template_name: Name of template from EMAIL_TEMPLATES
            to_emails: List of recipient email addresses
            template_data: Dictionary of data to populate template
            attachments: List of file paths to attach
            cc_emails: List of CC email addresses

        Returns:
            True if successful, False otherwise
        """
        if template_name not in EMAIL_TEMPLATES:
            logger.error(f"Template not found: {template_name}")
            return False

        template = EMAIL_TEMPLATES[template_name]

        # Add generated time if not present
        if 'generated_time' not in template_data:
            template_data['generated_time'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

        try:
            # Format subject and body
            subject = template['subject'].format(**template_data)
            body = template['body'].format(**template_data)

            return self.send_email(
                to_emails=to_emails,
                subject=subject,
                html_body=body,
                attachments=attachments,
                cc_emails=cc_emails
            )

        except KeyError as e:
            logger.error(f"Missing template data key: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending template email: {e}")
            return False

    def send_stage1_complete(self,
                            to_emails: List[str],
                            account_prefix: str,
                            timestamp: str,
                            output_files: Dict[str, Path],
                            stats: Dict,
                            file_filter: Dict[str, bool] = None) -> bool:
        """
        Send Stage 1 completion email

        Args:
            to_emails: Recipients
            account_prefix: Account prefix
            timestamp: Processing timestamp
            output_files: Dictionary of output files
            stats: Processing statistics
            file_filter: Dictionary of file type filters (csv, summary, missing, recon)
        """
        # Default: attach all files if no filter provided
        if file_filter is None:
            file_filter = {
                'csv': True,
                'summary': True,
                'missing': True,
                'recon': True
            }

        # Categorize files
        csv_files = ['parsed_trades', 'parsed_positions', 'final_positions', 'position_summary']
        summary_files = ['position_summary', 'enhanced_clearing']
        missing_files = ['missing_tickers', 'missing_strategies']
        recon_files = ['broker_recon_report', 'enhanced_clearing']

        # Create file list HTML and attachments
        file_list = ""
        attachments = []
        for file_type, file_path in output_files.items():
            if file_path and Path(file_path).exists():
                # Determine if this file should be included
                include = False
                if file_filter.get('csv', True) and file_type in csv_files:
                    include = True
                if file_filter.get('summary', True) and file_type in summary_files:
                    include = True
                if file_filter.get('missing', True) and file_type in missing_files:
                    include = True
                if file_filter.get('recon', True) and file_type in recon_files:
                    include = True

                # Always list in email body
                file_list += f"<li>{Path(file_path).name}</li>"

                # Only attach if included in filter
                if include:
                    # Check file size (skip files > 5MB)
                    file_size = Path(file_path).stat().st_size / (1024 * 1024)  # MB
                    if file_size <= 5:
                        attachments.append(file_path)

        # Format fund name and date
        fund_name = self._get_fund_name(account_prefix)
        date = datetime.now().strftime('%d/%m/%Y')

        template_data = {
            'fund_name': fund_name,
            'date': date,
            'account_prefix': account_prefix or '',
            'timestamp': timestamp,
            'total_trades': stats.get('total_trades', 0),
            'starting_positions': stats.get('starting_positions', 0),
            'final_positions': stats.get('final_positions', 0),
            'file_list': file_list
        }

        return self.send_from_template(
            template_name='stage1_complete',
            to_emails=to_emails,
            template_data=template_data,
            attachments=attachments
        )

    def send_deliverables_report(self,
                                to_emails: List[str],
                                account_prefix: str,
                                timestamp: str,
                                deliverables_file: Path,
                                stats: Dict) -> bool:
        """Send deliverables report email"""
        fund_name = self._get_fund_name(account_prefix)
        date = datetime.now().strftime('%d/%m/%Y')

        template_data = {
            'fund_name': fund_name,
            'date': date,
            'account_prefix': account_prefix or '',
            'timestamp': timestamp,
            'total_underlyings': stats.get('total_underlyings', 0),
            'total_deliverables': stats.get('total_deliverables', 0)
        }

        return self.send_from_template(
            template_name='deliverables_complete',
            to_emails=to_emails,
            template_data=template_data,
            attachments=[deliverables_file] if deliverables_file else None
        )

    def send_expiry_delivery(self,
                           to_emails: List[str],
                           expiry_date: str,
                           account_prefix: str,
                           output_files: List[Path],
                           stats: Dict) -> bool:
        """Send expiry delivery email"""
        fund_name = self._get_fund_name(account_prefix)

        template_data = {
            'fund_name': fund_name,
            'expiry_date': expiry_date,
            'account_prefix': account_prefix or '',
            'total_positions': stats.get('total_positions', 0),
            'cash_settlements': stats.get('cash_settlements', 0)
        }

        return self.send_from_template(
            template_name='expiry_delivery',
            to_emails=to_emails,
            template_data=template_data,
            attachments=output_files
        )

    def send_broker_recon(self,
                         to_emails: List[str],
                         timestamp: str,
                         recon_file: Path,
                         enhanced_file: Optional[Path],
                         stats: Dict,
                         account_prefix: str = '') -> bool:
        """Send broker reconciliation email"""
        attachments = [recon_file]
        if enhanced_file:
            attachments.append(enhanced_file)

        fund_name = self._get_fund_name(account_prefix)
        date = datetime.now().strftime('%d/%m/%Y')

        template_data = {
            'fund_name': fund_name,
            'date': date,
            'timestamp': timestamp,
            'matched_count': stats.get('matched_count', 0),
            'unmatched_clearing': stats.get('unmatched_clearing', 0),
            'unmatched_broker': stats.get('unmatched_broker', 0),
            'match_rate': stats.get('match_rate', 0)
        }

        return self.send_from_template(
            template_name='broker_recon',
            to_emails=to_emails,
            template_data=template_data,
            attachments=attachments
        )

    def send_error_notification(self,
                               to_emails: List[str],
                               error_type: str,
                               error_message: str,
                               error_trace: str = '',
                               account_prefix: str = '') -> bool:
        """Send error notification email"""
        fund_name = self._get_fund_name(account_prefix)
        date = datetime.now().strftime('%d/%m/%Y')

        template_data = {
            'fund_name': fund_name,
            'date': date,
            'error_type': error_type,
            'timestamp': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
            'error_message': error_message,
            'error_trace': error_trace or 'No trace available'
        }

        return self.send_from_template(
            template_name='error_notification',
            to_emails=to_emails,
            template_data=template_data
        )

    def _get_fund_name(self, account_prefix: str) -> str:
        """Get fund name from account prefix"""
        if not account_prefix:
            return "Trade Processing"

        # Remove trailing underscore
        prefix = account_prefix.rstrip('_')

        # Map prefixes to fund names
        fund_mapping = {
            'AURIGIN': 'Aurigin',
            'AURIGIN_': 'Aurigin',
            # Add more mappings as needed
        }

        return fund_mapping.get(prefix, prefix)

    def _get_mime_type(self, file_path: Path) -> str:
        """Get MIME type based on file extension"""
        extension = Path(file_path).suffix.lower()
        mime_types = {
            '.csv': 'text/csv',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.pdf': 'application/pdf',
            '.txt': 'text/plain',
            '.json': 'application/json',
            '.xml': 'application/xml'
        }
        return mime_types.get(extension, 'application/octet-stream')


def test_email_config():
    """Test email configuration"""
    config = EmailConfig.from_env()

    print("Email Configuration Test")
    print("=" * 50)
    print(f"Configured: {config.is_configured()}")
    print(f"API Key: {'***' + config.sendgrid_api_key[-4:] if config.sendgrid_api_key else 'NOT SET'}")
    print(f"From Email: {config.from_email or 'NOT SET'}")
    print(f"From Name: {config.from_name}")
    print("=" * 50)

    if config.is_configured():
        sender = EmailSender(config)
        print(f"\nEmail sending enabled: {sender.is_enabled()}")
    else:
        print("\nPlease set environment variables:")
        print("  - SENDGRID_API_KEY")
        print("  - SENDGRID_FROM_EMAIL")


if __name__ == "__main__":
    test_email_config()
