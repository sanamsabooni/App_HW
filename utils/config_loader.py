import os
from dotenv import load_dotenv

load_dotenv()

def get_env_variable(key):
    return os.getenv(key)
