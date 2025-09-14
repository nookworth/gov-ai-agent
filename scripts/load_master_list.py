import json
import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

load_dotenv()

MASTER_LIST_PATH = os.path.join("data", "2025_general_session_master_list.json")
TIDB_CONNECTION_STRING = os.getenv("TIDB_CONNECTION_STRING")

def parse_connection_string(connection_string):
    """Parse TiDB connection string to get connection parameters"""
    # Expected format: mysql://username:password@host:port/database?ssl_verify_cert=false&ssl_verify_identity=false
    import urllib.parse
    parsed = urllib.parse.urlparse(connection_string)

    return {
        'host': parsed.hostname,
        'port': parsed.port or 4000,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path[1:] if parsed.path else None,
        'ssl_disabled': 'ssl_verify_cert=false' in parsed.query
    }

def create_bills_table(cursor):
    """Create the bills table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bills (
        bill_id INT PRIMARY KEY,
        number VARCHAR(20) NOT NULL,
        change_hash VARCHAR(64),
        url TEXT,
        status_date DATE,
        status INT,
        last_action_date DATE,
        last_action TEXT,
        title TEXT,
        description TEXT,
        session_id INT,
        INDEX idx_number (number),
        INDEX idx_bill_id (bill_id)
    )
    """
    cursor.execute(create_table_query)
    print("Bills table created or already exists")

def load_master_list_to_tidb():
    """Load the master list JSON data into TiDB"""

    # Parse connection string
    conn_params = parse_connection_string(TIDB_CONNECTION_STRING)

    try:
        # Connect to TiDB
        connection = mysql.connector.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            user=conn_params['user'],
            password=conn_params['password'],
            database=conn_params['database'],
            ssl_disabled=conn_params['ssl_disabled']
        )

        cursor = connection.cursor()

        # Create table
        create_bills_table(cursor)

        # Load JSON data
        with open(MASTER_LIST_PATH, 'r') as f:
            data = json.load(f)

        session_info = data['masterlist']['session']
        session_id = session_info['session_id']

        # Prepare insert query
        insert_query = """
        INSERT INTO bills (
            bill_id, number, change_hash, url, status_date, status,
            last_action_date, last_action, title, description, session_id
        ) VALUES (
            %(bill_id)s, %(number)s, %(change_hash)s, %(url)s, %(status_date)s, %(status)s,
            %(last_action_date)s, %(last_action)s, %(title)s, %(description)s, %(session_id)s
        ) ON DUPLICATE KEY UPDATE
            change_hash = VALUES(change_hash),
            url = VALUES(url),
            status_date = VALUES(status_date),
            status = VALUES(status),
            last_action_date = VALUES(last_action_date),
            last_action = VALUES(last_action),
            title = VALUES(title),
            description = VALUES(description)
        """

        bills_inserted = 0

        # Insert bill data
        for key, bill_info in data['masterlist'].items():
            if key != 'session' and isinstance(bill_info, dict):
                bill_data = {
                    'bill_id': bill_info.get('bill_id'),
                    'number': bill_info.get('number'),
                    'change_hash': bill_info.get('change_hash'),
                    'url': bill_info.get('url'),
                    'status_date': bill_info.get('status_date'),
                    'status': bill_info.get('status'),
                    'last_action_date': bill_info.get('last_action_date'),
                    'last_action': bill_info.get('last_action'),
                    'title': bill_info.get('title'),
                    'description': bill_info.get('description'),
                    'session_id': session_id
                }

                cursor.execute(insert_query, bill_data)
                bills_inserted += 1

        connection.commit()
        print(f"Successfully inserted/updated {bills_inserted} bills")

    except Error as e:
        print(f"Error connecting to TiDB: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    load_master_list_to_tidb()