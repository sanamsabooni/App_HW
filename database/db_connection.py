import psycopg2
from utils.config_loader import get_env_variable

def get_db_connection():
    return psycopg2.connect(
        host=get_env_variable("RDS_HOST"),
        dbname=get_env_variable("RDS_DB"),
        user=get_env_variable("RDS_USER"),
        password=get_env_variable("RDS_PASSWORD")
    )
