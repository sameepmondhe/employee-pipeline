import subprocess
import sys
import os

REQUIRED_PACKAGES = [
    ("pytest", None),
    ("pyspark", "3.3.2"),
    ("delta-spark", "2.2.0"),
    ("pkg_resources", None)
]

REQUIRED_PYTHON_MAJOR = 3
REQUIRED_PYTHON_MINOR = 9


def check_and_install_package(pkg, version=None):
    try:
        import pkg_resources
        if version:
            installed_version = pkg_resources.get_distribution(pkg).version
            if installed_version == version:
                print(f"{pkg}=={version} is already installed.")
                return
            else:
                print(f"{pkg} version {installed_version} found, but {version} required. Upgrading...")
        else:
            __import__(pkg)
            print(f"{pkg} is already installed.")
            return
    except Exception:
        print(f"{pkg} not found. Installing...")
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

