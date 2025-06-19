import subprocess
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

bronze = os.path.join(PROJECT_ROOT, 'src', 'bronze', 'ingest_employees.py')
silver = os.path.join(PROJECT_ROOT, 'src', 'silver', 'validate_employees.py')
gold = os.path.join(PROJECT_ROOT, 'src', 'gold', 'curate_employees.py')

scripts = [bronze, silver, gold]

def run_script(script):
    """Run a Python script and print its output. Raise an exception if it fails."""
    print(f"\n=== Running {os.path.basename(script)} ===")
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Script {script} failed.")

def run_bronze():
    """Run Bronze ingestion step."""
    run_script(bronze)

def run_silver():
    """Run Silver validation step."""
    run_script(silver)

def run_gold():
    """Run Gold curation step."""
    run_script(gold)

def main():
    """Run the ETL pipeline: Bronze -> Silver -> Gold."""
    print("Starting Employee ETL Pipeline...\n")
    print("--- Bronze Stage ---")
    run_bronze()
    print("--- Silver Stage ---")
    run_silver()
    print("--- Gold Stage ---")
    run_gold()
    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
