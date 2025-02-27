import os
import psycopg2
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

# Load environment variables
load_dotenv()

SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

DB_HOST = os.getenv("RDS_HOST")
DB_PORT = int(os.getenv("RDS_PORT", 5432))
DB_NAME = os.getenv("RDS_DB")
DB_USER = os.getenv("RDS_USER")
DB_PASSWORD = os.getenv("RDS_PASSWORD")

def get_db_connection():
    """Establishes an SSH tunnel and connects to the PostgreSQL database."""
    try:
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(DB_HOST, DB_PORT)
        )
        tunnel.start()

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        return conn, tunnel
    except Exception as e:
        print(f"❌ Database connection error: {str(e)}")
        return None, None
