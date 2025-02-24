import psycopg2

try:
    conn = psycopg2.connect(
        host="hw-db-2025.cru0k6aaccv5.us-east-1.rds.amazonaws.com",
        database="ZohoCrmDB",
        user="Admin",
        password="your_password"
    )
    print("✅ Successfully connected to PostgreSQL!")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
