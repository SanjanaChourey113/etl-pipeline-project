import requests
import psycopg2
from requests.auth import HTTPDigestAuth

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="etl_project",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# LOOP through documents
for i in range(0, 50):

    order_url = f"http://localhost:8000/v1/documents?uri=/orders.json/{i}.json&format=json"
    payment_url = f"http://localhost:8000/v1/documents?uri=/payments.json/{i}.json&format=json"

    order_res = requests.get(order_url, auth=HTTPDigestAuth("sanjana", "Sanjana@22"))
    payment_res = requests.get(payment_url, auth=HTTPDigestAuth("sanjana", "Sanjana@22"))

    if order_res.status_code != 200 or payment_res.status_code != 200:
        continue

    order = order_res.json()
    payment = payment_res.json()

    # record_status logic
    status = "valid" if payment.get("payment_status") == "success" else "suspicious"

    cur.execute("""
        INSERT INTO orders_final (order_id, customer_id, order_status, record_status)
        VALUES (%s, %s, %s, %s)
    """, (
        order.get("order_id"),
        order.get("customer_id"),
        order.get("order_status"),
        status
    ))

conn.commit()
cur.close()
conn.close()

print(" Data loaded successfully!")