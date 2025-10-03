"""
Encrypted File Handler
Handles password-protected Excel files
"""

import pandas as pd
import io
import logging
from pathlib import Path
from typing import Optional, Union, Tuple
import tempfile
import os

try:
    import msoffcrypto
    MSOFFCRYPTO_AVAILABLE = True
except ImportError:
    MSOFFCRYPTO_AVAILABLE = False
    logging.warning("msoffcrypto-tool not installed. Password-protected Excel files won't be supported.")

logger = logging.getLogger(__name__)


def is_encrypted_excel(file_path: Union[str, Path, bytes, io.BytesIO]) -> bool:
    """Check if an Excel file is encrypted"""
    if not MSOFFCRYPTO_AVAILABLE:
        return False

    try:
        if isinstance(file_path, (str, Path)):
            with open(file_path, 'rb') as f:
                file = msoffcrypto.OfficeFile(f)
                return file.is_encrypted()
        else:
            # For file-like objects (BytesIO)
            if hasattr(file_path, 'seek'):
                file_path.seek(0)
            file = msoffcrypto.OfficeFile(file_path)
            is_enc = file.is_encrypted()
            if hasattr(file_path, 'seek'):
                file_path.seek(0)
            return is_enc
    except:
        return False


def decrypt_excel_file(file_input: Union[str, Path, bytes, io.BytesIO],
                      password: str) -> Tuple[bool, Optional[io.BytesIO], Optional[str]]:
    """
    Decrypt an encrypted Excel file
    Returns: (success, decrypted_file_buffer, error_message)
    """
    if not MSOFFCRYPTO_AVAILABLE:
        return False, None, "msoffcrypto-tool not installed"

    try:
        decrypted = io.BytesIO()

        if isinstance(file_input, (str, Path)):
            # File path provided
            with open(file_input, 'rb') as f:
                office_file = msoffcrypto.OfficeFile(f)
                office_file.load_key(password=password)
                office_file.decrypt(decrypted)
        else:
            # BytesIO or bytes provided
            if isinstance(file_input, bytes):
                file_input = io.BytesIO(file_input)

            file_input.seek(0)
            office_file = msoffcrypto.OfficeFile(file_input)
            office_file.load_key(password=password)
            office_file.decrypt(decrypted)
            file_input.seek(0)

        decrypted.seek(0)
        return True, decrypted, None

    except Exception as e:
        error_msg = str(e)
        if "incorrect password" in error_msg.lower():
            return False, None, "Incorrect password"
        else:
            return False, None, f"Decryption failed: {error_msg}"


def read_excel_with_password(file_input: Union[str, Path, bytes, io.BytesIO],
                           password: Optional[str] = None,
                           sheet_name: Union[str, int, None] = 0) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
    """
    Read Excel file, handling encryption if needed
    Returns: (success, dataframe, error_message)
    """
    try:
        # First check if file is encrypted
        if is_encrypted_excel(file_input):
            if not password:
                return False, None, "File is encrypted. Password required."

            # Try to decrypt
            success, decrypted_buffer, error = decrypt_excel_file(file_input, password)

            if not success:
                return False, None, error

            # Read the decrypted file
            try:
                df = pd.read_excel(decrypted_buffer, sheet_name=sheet_name)
                return True, df, None
            except Exception as e:
                return False, None, f"Error reading decrypted file: {str(e)}"
        else:
            # Not encrypted, read normally
            try:
                if isinstance(file_input, (str, Path)):
                    df = pd.read_excel(file_input, sheet_name=sheet_name)
                else:
                    df = pd.read_excel(file_input, sheet_name=sheet_name)
                return True, df, None
            except Exception as e:
                return False, None, f"Error reading file: {str(e)}"

    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def read_csv_or_excel_with_password(file_input: Union[str, Path, bytes, io.BytesIO],
                                   password: Optional[str] = None) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
    """
    Read CSV or Excel file, handling encryption for Excel files
    Returns: (success, dataframe, error_message)
    """
    # Determine file type
    if isinstance(file_input, (str, Path)):
        file_name = str(file_input)
    else:
        # For BytesIO, check if we have a name attribute
        file_name = getattr(file_input, 'name', 'unknown.xlsx')

    if file_name.lower().endswith('.csv'):
        # CSV files are not encrypted
        try:
            if isinstance(file_input, (str, Path)):
                df = pd.read_csv(file_input)
            else:
                df = pd.read_csv(file_input)
            return True, df, None
        except Exception as e:
            return False, None, f"Error reading CSV: {str(e)}"
    else:
        # Assume Excel format
        return read_excel_with_password(file_input, password)