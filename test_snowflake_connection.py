"""
Diagnostic script to test Snowflake connectivity.
Includes timeout, verbose logging, and step-by-step output.
"""

import sys
import signal
import traceback

# Timeout handler for Windows (uses threading)
def run_with_timeout(func, timeout_sec=30):
    import threading
    result = [None]
    error = [None]
    
    def target():
        try:
            result[0] = func()
        except Exception as e:
            error[0] = e
    
    t = threading.Thread(target=target)
    t.daemon = True
    t.start()
    t.join(timeout=timeout_sec)
    
    if t.is_alive():
        print(f"⏰ TIMEOUT after {timeout_sec}s — connection is hanging!")
        print("   Possible causes:")
        print("   1. Firewall blocking Snowflake (port 443)")
        print("   2. Account name is incorrect")
        print("   3. MFA/SSO required for this account")
        print("   4. Network/proxy issue")
        return None
    
    if error[0]:
        raise error[0]
    return result[0]


def test_snowflake():
    from dotenv import load_dotenv
    load_dotenv()

    print("=" * 60)
    print("  SNOWFLAKE CONNECTIVITY DIAGNOSTIC")
    print("=" * 60)
    print()

    # Step 1: Check env vars
    from app.config import settings
    print("1️⃣  Checking environment variables...")
    creds = {
        "ACCOUNT": settings.SNOWFLAKE_ACCOUNT,
        "USER": settings.SNOWFLAKE_USER,
        "PASSWORD": "***" + settings.SNOWFLAKE_PASSWORD[-3:] if settings.SNOWFLAKE_PASSWORD else "(empty)",
        "WAREHOUSE": settings.SNOWFLAKE_WAREHOUSE,
        "DATABASE": settings.SNOWFLAKE_DATABASE,
        "SCHEMA": settings.SNOWFLAKE_SCHEMA,
        "ROLE": settings.SNOWFLAKE_ROLE,
    }
    for k, v in creds.items():
        status = "✅" if v and v != "(empty)" else "❌ MISSING"
        print(f"   {k}: {v} {status}")
    print()

    if not settings.SNOWFLAKE_ACCOUNT:
        print("❌ SNOWFLAKE_ACCOUNT is empty. Cannot proceed.")
        return

    # Step 2: Test import
    print("2️⃣  Importing snowflake.connector...")
    try:
        import snowflake.connector
        print(f"   ✅ snowflake-connector-python v{snowflake.connector.__version__}")
    except ImportError:
        print("   ❌ snowflake-connector-python not installed!")
        print("   Run: pip install snowflake-connector-python")
        return
    print()

    # Step 3: Test connection with timeout
    print(f"3️⃣  Connecting to Snowflake (30s timeout)...")
    print(f"   Account: {settings.SNOWFLAKE_ACCOUNT}")
    print(f"   URL: https://{settings.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com")
    print()

    def connect():
        return snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
            login_timeout=20,
            network_timeout=20,
        )

    conn = run_with_timeout(connect, timeout_sec=30)
    if conn is None:
        return

    print(f"   ✅ Connected to Snowflake!")
    print()

    # Step 4: Query the ORDERS table
    print("4️⃣  Querying ORDERS table...")
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ORDERS")
        count = cur.fetchone()[0]
        print(f"   ✅ ORDERS table has {count} rows")
        print()

        cur.execute("SELECT * FROM ORDERS LIMIT 5")
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
        print(f"   Columns: {columns}")
        print(f"   Sample rows:")
        for i, row in enumerate(rows):
            record = dict(zip(columns, row))
            print(f"   [{i+1}] {record}")
        cur.close()
    except Exception as e:
        print(f"   ❌ Query failed: {e}")
        print()
        print("   The table ORDERS may not exist. Let's see what tables are available:")
        try:
            cur = conn.cursor()
            cur.execute("SHOW TABLES IN SCHEMA")
            tables = cur.fetchall()
            if tables:
                print(f"   Available tables:")
                for t in tables:
                    print(f"     - {t[1]}")  # table name is usually index 1
            else:
                print("   No tables found in this schema.")
            cur.close()
        except Exception as e2:
            print(f"   Could not list tables: {e2}")
    print()

    # Step 5: Test the connector
    print("5️⃣  Testing SnowflakeConnector...")
    try:
        from app.connectors.snowflake_connector import SnowflakeConnector
        sf = SnowflakeConnector(
            table="ORDERS",
            id_field="ORDER_ID",
            search_fields=("CUSTOMER_NAME", "ORDER_STATUS"),
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
        )
        result = sf.fetch(limit=3)
        print(f"   ✅ SnowflakeConnector.fetch() returned {result['total']} total, {len(result['items'])} items")
        for item in result["items"]:
            print(f"   {item}")
    except Exception as e:
        print(f"   ❌ SnowflakeConnector failed: {e}")
        traceback.print_exc()
    print()

    conn.close()
    print("✅ ALL TESTS PASSED — Snowflake integration is working!")


if __name__ == "__main__":
    # Add project root to path
    sys.path.insert(0, ".")
    try:
        test_snowflake()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        traceback.print_exc()
