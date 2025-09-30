import clickhouse_connect
import os
from dotenv import load_dotenv

def get_client():
    """
    Create and return a ClickHouse client using environment variables for configuration.
    """
    load_dotenv()
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', 8123)),
        user=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', '')
    )
    return client
