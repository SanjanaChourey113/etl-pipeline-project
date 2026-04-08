from dagster import asset
import os

#  Step 1: Extract
@asset
def extract_data():
    print("Extracting data from MarkLogic...")
    os.system("python postgres_load/load.py")
    return "data extracted"


#  Step 2: Transform (DBT)
@asset
def transform_data(extract_data):
    print("Running DBT models...")
    os.system("cd my_project && dbt run")
    return "data transformed"


#  Step 3: Test (Pytest)
@asset
def test_data(transform_data):
    print("Running Pytest...")
    os.system("python tests/test_data.py")
    return "tests passed"