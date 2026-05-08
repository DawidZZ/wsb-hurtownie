import pymssql

try:
    conn = pymssql.connect(
        host='localhost',
        user='sa',
        password='P@ssw0rd123!',
        database='master'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    version = cursor.fetchone()[0]
    print(version)
    conn.close()
    print("\n✓ SQL Server connection SUCCESS")
except Exception as e:
    print(f"✗ Connection FAILED: {e}")
