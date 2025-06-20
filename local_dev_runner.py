import subprocess
import sys
import os
from datetime import datetime

LOG_DIR = "logs"
TEST_LOG = os.path.join(LOG_DIR, "test.log")
PIPELINE_LOG = os.path.join(LOG_DIR, "pipeline.log")

os.makedirs(LOG_DIR, exist_ok=True)

def run_and_log(cmd, log_file, description):
    with open(log_file, "w") as f:
        f.write(f"=== {description} started at {datetime.now()} ===\n")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            f.write(line)
            print(line, end="")
        process.wait()
        f.write(f"\n=== {description} ended at {datetime.now()} ===\n")
        return process.returncode

def main():
    print("Running tests...")
    test_code = run_and_log([sys.executable, '-m', 'pytest', 'tests/'], TEST_LOG, "Pytest")
    if test_code != 0:
        print(f"Tests failed. See {TEST_LOG} for details.")
        sys.exit(test_code)
    print(f"Tests passed. See {TEST_LOG} for details.")

    print("\nRunning ETL pipeline locally...")
    pipeline_code = run_and_log([sys.executable, 'employees_etl.py', '--env', 'local'], PIPELINE_LOG, "ETL Pipeline")
    if pipeline_code != 0:
        print(f"ETL pipeline failed. See {PIPELINE_LOG} for details.")
        sys.exit(pipeline_code)
    print(f"ETL pipeline completed successfully. See {PIPELINE_LOG} for details.")

if __name__ == "__main__":
    main()

