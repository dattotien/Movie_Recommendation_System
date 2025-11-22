import sys
import time
from cassandra.cluster import Cluster, RetryPolicy
from cassandra.policies import ConstantReconnectionPolicy

# --- Cấu hình ---
CASSANDRA_HOST = 'cassandra' 
CASSANDRA_PORT = 9042
KEYSPACE = 'movie_recs'
TABLE_NAME = 'user_recommendations'

# SỬA 1: Giữ một session global (toàn cục) để tái sử dụng
_session = None

def get_cassandra_session():
    """
    Kết nối đến Cassandra và trả về một session.
    Sẽ thử kết nối lại nếu thất bại (rất quan trọng khi docker khởi động).
    Sử dụng lại session global nếu đã kết nối.
    """
    global _session
    # Nếu session đã có và chưa tắt, dùng lại nó
    if _session and not _session.is_shutdown:
        return _session

    cluster = Cluster(
        [CASSANDRA_HOST], 
        port=CASSANDRA_PORT,
        reconnection_policy=ConstantReconnectionPolicy(delay=5.0, max_attempts=10),
        default_retry_policy=RetryPolicy()
    )
    
    # Thử kết nối nhiều lần
    for i in range(10): # Thử 10 lần
        try:
            _session = cluster.connect()
            print(f"Successfully connected to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return _session
        except Exception as e:
            print(f"Error connecting to Cassandra (Attempt {i+1}/10): {e}", file=sys.stderr)
            time.sleep(5) # Chờ 5 giây rồi thử lại
            
    print("Failed to connect to Cassandra after 10 attempts.", file=sys.stderr)
    return None

def create_keyspace_and_table():
    """
    Chạy 1 lần để tạo Keyspace (giống DB) và Bảng (Table).
    (Hàm này được gọi bởi app.py khi khởi động)
    """
    session = get_cassandra_session()
    if session is None:
        print("Cannot connect to Cassandra, skipping keyspace/table creation.", file=sys.stderr)
        return

    try:
        # 1. Tạo Keyspace (Database)
        session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
        """)
        print(f"Keyspace '{KEYSPACE}' created or already exists.")
        
        # 2. Sử dụng Keyspace
        session.set_keyspace(KEYSPACE)
        
        # 3. Tạo Bảng (Table)
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            user_id text PRIMARY KEY,
            top_10 list<text>
        );
        """)
        print(f"Table '{TABLE_NAME}' created or already exists.")
        
    except Exception as e:
        print(f"Error setting up keyspace/table: {e}", file=sys.stderr)
    # Lưu ý: Không shutdown session ở đây để Lớp Speed/Batch có thể tái sử dụng

# SỬA 2: Hàm write_recs phải NHẬN session làm tham số
def write_recs(session, user_id, recs_list):
    """
    Ghi/Ghi đè (Upsert) danh sách gợi ý cho 1 user.
    (Hàm này cho Người 1 và Người 2 dùng)
    Lưu ý: session được truyền vào từ hàm process_batch
    """
    if session is None: 
        print("Cassandra session is None, skipping write.", file=sys.stderr)
        return
    
    try:
        session.set_keyspace(KEYSPACE)
        query = f"INSERT INTO {TABLE_NAME} (user_id, top_10) VALUES (?, ?)"
        prepared = session.prepare(query)
        # Đảm bảo recs_list là list of strings
        recs_list_str = [str(rec) for rec in recs_list]
        session.execute(prepared, (str(user_id), recs_list_str)) # Chuyển user_id sang str
    except Exception as e:
        print(f"Error writing recommendations for {user_id}: {e}", file=sys.stderr)
    # SỬA 3: KHÔNG được shutdown session ở đây, vì nó đang được dùng chung

def read_recs(user_id):
    """
    Đọc danh sách gợi ý cho 1 user.
    (Hàm này cho Người 3 dùng trong Web App)
    """
    session = get_cassandra_session() # Dùng chung session
    if session is None: return []

    try:
        session.set_keyspace(KEYSPACE)
        query = f"SELECT top_10 FROM {TABLE_NAME} WHERE user_id = ?"
        prepared = session.prepare(query)
        rows = session.execute(prepared, (str(user_id),)) # Chuyển user_id sang str
        
        row = rows.one() # Lấy hàng đầu tiên
        if row and row.top_10:
            return row.top_10 # Trả về list
        else:
            return [] # Không tìm thấy user
            
    except Exception as e:
        print(f"Error reading recommendations for {user_id}: {e}", file=sys.stderr)
        return []
    # Lưu ý: Không shutdown session ở đây

if __name__ == '__main__':
    # Chạy file này trực tiếp để setup và test:
    # docker compose exec app python src/utils/Cassandra_connector.py
    print("Setting up Cassandra keyspace and table...")
    create_keyspace_and_table()
    
    print("\nTesting write and read...")
    test_user = 'user_cassandra_test'
    test_recs = ['movie_1', 'movie_5', 'movie_99']
    
    # SỬA 4: Lấy session global để test
    test_session = get_cassandra_session()
    if test_session:
        # Truyền session vào
        write_recs(test_session, test_user, test_recs)
        print(f"Wrote test data for {test_user}")
        
        recs = read_recs(test_user)
        print(f"Read back test data: {recs}")
        
        if recs == test_recs:
            print("Test SUCCESSFUL!")
        else:
            print("Test FAILED!")
        
        # Đóng session sau khi test xong
        test_session.shutdown()
        print("Test session closed.")