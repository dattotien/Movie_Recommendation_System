import sys
import time
from cassandra.cluster import Cluster, RetryPolicy
from cassandra.policies import ConstantReconnectionPolicy

# --- Cấu hình ---
CASSANDRA_HOST = 'cassandra' 
CASSANDRA_PORT = 9042
KEYSPACE = 'movie_recs'
TABLE_NAME = 'user_recommendations'

_session = None

def get_cassandra_session():
    global _session
    if _session and not _session.is_shutdown:
        return _session

    for i in range(10): 
        cluster = None
        try:
            cluster = Cluster(
                [CASSANDRA_HOST], 
                port=CASSANDRA_PORT,
                reconnection_policy=ConstantReconnectionPolicy(delay=5.0, max_attempts=10),
                default_retry_policy=RetryPolicy()
            )
            _session = cluster.connect()
            print(f"Successfully connected to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return _session
        except Exception as e:
            print(f"Error connecting to Cassandra (Attempt {i+1}/10): {e}", file=sys.stderr)
            if cluster is not None:
                try:
                    cluster.shutdown()
                except:
                    pass
            time.sleep(5) 
            
    print("Failed to connect to Cassandra after 10 attempts.", file=sys.stderr)
    return None

def create_keyspace_and_table():
    session = get_cassandra_session()
    if session is None:
        print("Cannot connect to Cassandra, skipping keyspace/table creation.", file=sys.stderr)
        return

    try:
        session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
        """)
        print(f"Keyspace '{KEYSPACE}' created or already exists.")
        
        session.set_keyspace(KEYSPACE)
        
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            user_id text PRIMARY KEY,
            top_10 list<text>
        );
        """)
        print(f"Table '{TABLE_NAME}' created or already exists.")
        
    except Exception as e:
        print(f"Error setting up keyspace/table: {e}", file=sys.stderr)

def write_recs(session, user_id, recs_list):
    if session is None: 
        print("Cassandra session is None, skipping write.", file=sys.stderr)
        return
    
    try:
        session.set_keyspace(KEYSPACE)
        query = f"INSERT INTO {TABLE_NAME} (user_id, top_10) VALUES (?, ?)"
        prepared = session.prepare(query) 
        recs_list_str = [str(rec) for rec in recs_list]
        session.execute(prepared, (str(user_id), recs_list_str)) 
    except Exception as e:
        print(f"Error writing recommendations for {user_id}: {e}", file=sys.stderr)


def read_recs(user_id):
    session = get_cassandra_session() 
    if session is None: return []

    try:
        session.set_keyspace(KEYSPACE)
        query = f"SELECT top_10 FROM {TABLE_NAME} WHERE user_id = ?"
        prepared = session.prepare(query)
        rows = session.execute(prepared, (str(user_id),)) 
        
        row = rows.one() 
        if row and row.top_10:
            return row.top_10 
        else:
            return [] 
            
    except Exception as e:
        print(f"Error reading recommendations for {user_id}: {e}", file=sys.stderr)
        return []

if __name__ == '__main__':
    print("Setting up Cassandra keyspace and table...")
    create_keyspace_and_table()
    
    print("\nTesting write and read...")
    test_user = 'user_cassandra_test'
    test_recs = ['movie_1', 'movie_5', 'movie_99']
 
    test_session = get_cassandra_session()
    if test_session:
        write_recs(test_session, test_user, test_recs)
        print(f"Wrote test data for {test_user}")
        
        recs = read_recs(test_user)
        print(f"Read back test data: {recs}")
        
        if recs == test_recs:
            print("Test SUCCESSFUL!")
        else:
            print("Test FAILED!")

        test_session.shutdown()
        print("Test session closed.")