import psycopg2
import config

def get_db_connection():
    """Establishes connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=config.DB_HOST,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        print("✅ Database connected.")
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return None
