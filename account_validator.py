"""
Account Validator Module
Detects and validates CP codes across position and trade files
"""

import io
import logging
from typing import Optional, Tuple, Dict
import pandas as pd
from account_config import ACCOUNT_REGISTRY, get_account_by_cp_code, get_all_cp_codes

# Import encrypted file handler
try:
    from encrypted_file_handler import decrypt_excel_file, read_csv_or_excel_with_password
    ENCRYPTION_SUPPORT = True
except ImportError:
    ENCRYPTION_SUPPORT = False

logger = logging.getLogger(__name__)


class AccountValidator:
    """Validates account consistency across files"""

    def __init__(self):
        self.position_account = None
        self.trade_account = None
        self.validation_errors = []

    def detect_account_in_file(self, file_obj, file_type: str = "unknown") -> Optional[Dict]:
        """
        Detect CP code in file - uses Stage 2's EXACT decryption method

        Args:
            file_obj: File object (UploadedFile or similar)
            file_type: "position" or "trade" for logging

        Returns:
            Account dict if found, None otherwise
        """
        try:
            logger.info(f"Account detection for {file_type}: content_type={type(file_obj).__name__}")

            df = None

            # Try BOTH known passwords in order, using Stage 2's exact method
            KNOWN_PASSWORDS = ["Aurigin2017", "Aurigin2024"]

            if ENCRYPTION_SUPPORT:
                for password in KNOWN_PASSWORDS:
                    try:
                        file_obj.seek(0)
                        logger.info(f"Trying password '{password}' for {file_type} file")
                        success, df, error = read_csv_or_excel_with_password(file_obj, password)

                        if success and df is not None:
                            logger.info(f"✓ Successfully read {file_type} file with '{password}' - {len(df)} rows, {len(df.columns)} columns")
                            break
                        else:
                            logger.debug(f"Password '{password}' failed: {error}")
                            df = None
                    except Exception as e:
                        logger.debug(f"Password '{password}' exception: {e}")
                        df = None
                        continue

            # If passwords didn't work, try reading without password (unencrypted file)
            if df is None:
                try:
                    file_obj.seek(0)
                    logger.info(f"Trying to read {file_type} file without password")
                    success, df, error = read_csv_or_excel_with_password(file_obj, None)
                    if success and df is not None:
                        logger.info(f"✓ Read {file_type} file without password - {len(df)} rows, {len(df.columns)} columns")
                    else:
                        logger.warning(f"Failed to read {file_type} file: {error}")
                except Exception as e:
                    logger.warning(f"Could not read {file_type} file: {e}")

            # If we successfully read as DataFrame, search all cells
            search_text = ""
            if df is not None:
                # Log columns found
                logger.info(f"{file_type} file has columns: {list(df.columns)}")

                # Convert ALL cells to string and concatenate
                for col in df.columns:
                    # Convert each column to string and join
                    col_text = df[col].astype(str).str.cat(sep=' ')
                    search_text += " " + col_text

                    # Log if this column might have CP codes
                    if 'CP' in str(col).upper() or 'CODE' in str(col).upper():
                        sample_values = df[col].head(3).tolist()
                        logger.info(f"  Column '{col}' sample values: {sample_values}")

                logger.info(f"Created search text from {len(df.columns)} columns, {len(df)} rows, total length: {len(search_text)} chars")
            else:
                logger.warning(f"Could not read {file_type} file - no DataFrame created")
                return None

            # Normalize text for searching: uppercase
            search_text_upper = search_text.upper()

            # Debug: Log search text sample (first 500 chars)
            logger.debug(f"Search text sample for {file_type}: {search_text_upper[:500]}")

            # Search for each known CP code (case-insensitive)
            found_codes = []
            all_cp_codes = get_all_cp_codes()
            logger.info(f"Searching for {len(all_cp_codes)} known CP codes in {file_type} file: {all_cp_codes}")

            for cp_code in all_cp_codes:
                # Check if CP code appears in text (case-insensitive)
                # Also try without spaces/special chars to catch formatting variations
                cp_code_normalized = cp_code.upper().replace(' ', '').replace('-', '')
                search_normalized = search_text_upper.replace(' ', '').replace('-', '')

                if cp_code.upper() in search_text_upper or cp_code_normalized in search_normalized:
                    found_codes.append(cp_code)
                    logger.info(f"✓ Found CP code {cp_code} in {file_type} file")
                else:
                    logger.debug(f"✗ CP code {cp_code} not found in {file_type} file")

            # Validation
            if len(found_codes) == 0:
                logger.warning(f"No CP code found in {file_type} file")
                return None

            if len(found_codes) > 1:
                logger.error(f"Multiple CP codes found in {file_type} file: {found_codes}")
                self.validation_errors.append(
                    f"Multiple accounts detected in {file_type} file: {', '.join(found_codes)}"
                )
                return None

            # Single CP code found
            cp_code = found_codes[0]
            account = get_account_by_cp_code(cp_code)
            logger.info(f"Detected account in {file_type} file: {account['name']} ({cp_code})")
            return account

        except Exception as e:
            logger.error(f"Error detecting account in {file_type} file: {e}")
            return None

    def detect_account_in_position_file(self, file_obj) -> Optional[Dict]:
        """
        Detect account in position file (tries known passwords automatically)

        Args:
            file_obj: Uploaded file object from streamlit

        Returns:
            Account dict if found, None otherwise
        """
        try:
            file_obj.seek(0)
            account = self.detect_account_in_file(file_obj, "position")
            file_obj.seek(0)  # Reset for later use
            self.position_account = account
            return account

        except Exception as e:
            logger.error(f"Error reading position file for account detection: {e}")
            return None

    def detect_account_in_trade_file(self, file_obj) -> Optional[Dict]:
        """
        Detect account in trade file (tries known passwords automatically)

        Args:
            file_obj: Uploaded file object from streamlit

        Returns:
            Account dict if found, None otherwise
        """
        try:
            file_obj.seek(0)
            account = self.detect_account_in_file(file_obj, "trade")
            file_obj.seek(0)  # Reset for later use
            self.trade_account = account
            return account

        except Exception as e:
            logger.error(f"Error reading trade file for account detection: {e}")
            return None

    def validate_account_match(self) -> Tuple[bool, str, str]:
        """
        Validate that position and trade files are from same account

        Returns:
            (is_valid, status_type, message)
            status_type: "success", "warning", "error"
        """
        pos_acc = self.position_account
        trade_acc = self.trade_account

        # Case 1: Both files have same account (GOOD!)
        if pos_acc and trade_acc and pos_acc['cp_code'] == trade_acc['cp_code']:
            return True, "success", f"✅ Account validated: {pos_acc['name']} ({pos_acc['cp_code']})"

        # Case 2: Mismatch between known accounts (BLOCK!)
        if pos_acc and trade_acc and pos_acc['cp_code'] != trade_acc['cp_code']:
            msg = (
                f"❌ ACCOUNT MISMATCH DETECTED!\n\n"
                f"Position File: {pos_acc['name']} ({pos_acc['cp_code']})\n"
                f"Trade File: {trade_acc['name']} ({trade_acc['cp_code']})\n\n"
                f"Cannot process files from different accounts.\n"
                f"Please upload matching files."
            )
            return False, "error", msg

        # Case 3: Multiple accounts in one file (BLOCK!)
        if self.validation_errors:
            return False, "error", "\n".join(self.validation_errors)

        # Case 4: One file has account, other doesn't (WARN)
        if pos_acc and not trade_acc:
            msg = (
                f"⚠️ Account detected in position file only: {pos_acc['name']} ({pos_acc['cp_code']})\n"
                f"Trade file CP code not found. Proceeding with caution."
            )
            return True, "warning", msg

        if trade_acc and not pos_acc:
            msg = (
                f"⚠️ Account detected in trade file only: {trade_acc['name']} ({trade_acc['cp_code']})\n"
                f"Position file CP code not found. Proceeding with caution."
            )
            return True, "warning", msg

        # Case 5: Neither file has detectable account (WARN)
        if not pos_acc and not trade_acc:
            msg = (
                f"⚠️ No CP code detected in either file.\n"
                f"Cannot verify account consistency. Proceeding with caution."
            )
            return True, "warning", msg

        # Default fallback
        return True, "warning", "Account validation inconclusive"

    def get_account_info(self) -> Optional[Dict]:
        """Get the detected account info (position file takes precedence)"""
        return self.position_account or self.trade_account

    def get_account_prefix(self) -> str:
        """Get account name for file prefixing, returns empty string if unknown"""
        account = self.get_account_info()
        return f"{account['name']}_" if account else ""

    def reset(self):
        """Reset validator state"""
        self.position_account = None
        self.trade_account = None
        self.validation_errors = []
