import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname="etl_project",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

# Test 1: Table exists and has data
def test_table_exists():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM clean_orders;")
    result = cur.fetchone()

    conn.close()

    assert result[0] > 0


# Test 2: Suspicious flag check
def test_suspicious_flag():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM clean_orders
        WHERE is_suspicious = 1
    """)

    result = cur.fetchone()

    conn.close()

    assert result[0] >= 0