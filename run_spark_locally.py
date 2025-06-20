import os
import subprocess
import sys
import platform

def check_python_version():
    required_major = 3
    required_minor = 9
    if sys.version_info.major != required_major or sys.version_info.minor < required_minor:
        print(f"Python {required_major}.{required_minor}+ is required. Current version: {sys.version}")
        sys.exit(1)
    print(f"Python version OK: {sys.version}")

def check_pip_package(pkg, required_version):
    try:
        import pkg_resources
        version = pkg_resources.get_distribution(pkg).version
        if version != required_version:
            print(f"{pkg} version {required_version} is required. Found: {version}")
            sys.exit(1)
        print(f"{pkg} version OK: {version}")
    except Exception as e:
        print(f"{pkg} is not installed or not found: {e}")
        sys.exit(1)

def check_java_home(java_version="11"):
    # Check if 'java -version' returns the correct version
    try:
        version_output = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT)
        version_str = version_output.decode('utf-8')
        if not (f'version "{java_version}' in version_str):
            print(f"ERROR: 'java -version' does not return Java {java_version}. Current version output:")
            print(version_str)
            print("If you use jenv, run 'jenv local 11' in your project directory.")
            sys.exit(1)
        print(f"Java version OK: {version_str.splitlines()[0]}")
    except Exception as e:
        print(f"Could not determine Java version: {e}")
        sys.exit(1)

def main():
    check_python_version()
    check_pip_package('pyspark', '3.3.2')
    check_pip_package('delta-spark', '2.2.0')
    check_java_home("11")
    print("\nAll local environment checks passed. You can now run employees_etl.py with --env local.")

if __name__ == "__main__":
    main()
