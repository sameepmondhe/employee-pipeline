import subprocess
import sys
import os
import importlib.util

REQUIRED_PACKAGES = [
    ("pytest", None),
    ("pyspark", "3.3.2"),
    ("delta-spark", "2.2.0"),
    ("pandas", None)
]

REQUIRED_PYTHON_MAJOR = 3
REQUIRED_PYTHON_MINOR = 9


def check_and_install_package(pkg, version=None):
    try:        # Use importlib.metadata for version check, but handle import name vs. package name
        import importlib.metadata
        try:
            installed_version = importlib.metadata.version(pkg)
            if version:
                if installed_version == version:
                    print(f"{pkg}=={version} is already installed.")
                    return
                else:
                    print(f"{pkg} version {installed_version} found, but {version} required. Upgrading...")
            else:
                print(f"{pkg} is already installed.")
                return
        except importlib.metadata.PackageNotFoundError:
            print(f"{pkg} not found. Installing...")
    except Exception as e:
        print(f"Error checking {pkg}: {e}. Attempting install...")
    # Install or upgrade
    pip_args = [sys.executable, '-m', 'pip', 'install']
    if version:
        pip_args.append(f"{pkg}=={version}")
    else:
        pip_args.append(pkg)
    subprocess.check_call(pip_args)


def check_python_version():
    if sys.version_info.major != REQUIRED_PYTHON_MAJOR or sys.version_info.minor < REQUIRED_PYTHON_MINOR:
        print(f"Python {REQUIRED_PYTHON_MAJOR}.{REQUIRED_PYTHON_MINOR}+ is required. Current version: {sys.version}")
        sys.exit(1)
    print(f"Python version OK: {sys.version}")


def main():
    print("Checking Python version...")
    check_python_version()
    print("\nChecking and installing required packages...")
    for pkg, version in REQUIRED_PACKAGES:
        check_and_install_package(pkg, version)
    print("\nAll required software is installed and up to date.")

if __name__ == "__main__":
    main()
