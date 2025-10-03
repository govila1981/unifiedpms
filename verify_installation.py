"""
Installation Verification Script
Checks all dependencies are properly installed
"""

import sys
import importlib.metadata

def check_module(module_name, import_name=None):
    """Check if a module is installed and can be imported"""
    if import_name is None:
        import_name = module_name

    try:
        # Try to import the module
        __import__(import_name)

        # Get version if available
        try:
            version = importlib.metadata.version(module_name)
            return True, version
        except:
            return True, "Unknown"
    except ImportError:
        return False, None

def main():
    print("=" * 60)
    print("TRADE PROCESSING PIPELINE - DEPENDENCY CHECK")
    print("=" * 60)
    print()

    # Define all required modules
    modules = [
        # (package_name, import_name, description, critical)
        ("streamlit", "streamlit", "Core web framework", True),
        ("pandas", "pandas", "Data processing", True),
        ("numpy", "numpy", "Numerical operations", True),
        ("openpyxl", "openpyxl", "Excel .xlsx support", True),
        ("xlrd", "xlrd", "Excel .xls support", True),
        ("xlsxwriter", "xlsxwriter", "Excel writing", True),
        ("msoffcrypto-tool", "msoffcrypto", "Password-protected Excel", True),
        ("yfinance", "yfinance", "Yahoo Finance data", True),
        ("pytz", "pytz", "Timezone support", True),
        ("python-dateutil", "dateutil", "Date parsing", True),
        ("psutil", "psutil", "System utilities", False),
        ("requests", "requests", "HTTP requests", False),
        ("urllib3", "urllib3", "URL handling", False),
    ]

    critical_missing = []
    optional_missing = []
    installed = []

    print("Checking dependencies...\n")

    for package_name, import_name, description, critical in modules:
        is_installed, version = check_module(package_name, import_name)

        if is_installed:
            status = f"✓ {package_name:<20} {version:<12} - {description}"
            print(status)
            installed.append(package_name)
        else:
            status = f"✗ {package_name:<20} {'Missing':<12} - {description}"
            print(status)
            if critical:
                critical_missing.append(package_name)
            else:
                optional_missing.append(package_name)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print(f"\n✓ Installed packages: {len(installed)}")

    if critical_missing:
        print(f"\n❌ CRITICAL missing packages: {len(critical_missing)}")
        print("   " + ", ".join(critical_missing))
        print("\n   These packages are required for basic functionality.")
        print("   Run: pip install -r requirements.txt")

    if optional_missing:
        print(f"\n⚠️  Optional missing packages: {len(optional_missing)}")
        print("   " + ", ".join(optional_missing))
        print("\n   These packages enhance functionality but are not critical.")

    # Check Python version
    python_version = sys.version_info
    print(f"\nPython version: {python_version.major}.{python_version.minor}.{python_version.micro}")

    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
        print("⚠️  WARNING: Python 3.8 or higher is recommended")
    else:
        print("✓ Python version is compatible")

    # Final status
    print("\n" + "=" * 60)
    if not critical_missing:
        print("✅ ALL CRITICAL DEPENDENCIES ARE INSTALLED")
        print("The application should run correctly.")
        return 0
    else:
        print("❌ MISSING CRITICAL DEPENDENCIES")
        print("Please install missing packages before running the application.")
        return 1

if __name__ == "__main__":
    sys.exit(main())