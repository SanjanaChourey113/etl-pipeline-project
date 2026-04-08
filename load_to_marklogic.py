import requests
import json
import os
from requests.auth import HTTPDigestAuth

# Config
MARKLOGIC_URL = "http://localhost:8000/v1/documents"
USERNAME = "sanjana"
PASSWORD = "Sanjana@22"

DATA_FOLDER = "data"

# Mark Logic Function
def apply_mark_logic(data):
    seen_ids = set()

    for record in data:
        record["record_status"] = "valid"

        if not record.get("customer_id") or not record.get("order_id"):
            record["record_status"] = "invalid"

        elif record["customer_id"] in seen_ids:
            record["record_status"] = "duplicate"

        elif record.get("order_amount", 0) > 1000:
            record["record_status"] = "high_value"

        elif record.get("order_amount", 0) < 0:
            record["record_status"] = "suspicious"

        seen_ids.add(record.get("customer_id"))

    return data


# Load to MarkLogic (Curated)
def load_to_marklogic(file_name):
    file_path = os.path.join(DATA_FOLDER, file_name)

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Apply mark logic
    data = apply_mark_logic(data)

    for i, record in enumerate(data):
        uri = f"/curated/{file_name}/{i}.json"

        response = requests.put(
            f"{MARKLOGIC_URL}?uri={uri}",
            json=record,
            auth=HTTPDigestAuth(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"}
        )

        print(f"{file_name} curated {i} -> {response.status_code}")


# Run
files = ["orders.json", "customer.json"]

for file in files:
    load_to_marklogic(file)