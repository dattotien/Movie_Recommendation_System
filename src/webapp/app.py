from flask import Flask, render_template, request
import sys
# IMPORT file CASSANDRA connector mới
# Cách import này sẽ hoạt động vì docker-compose chạy
# lệnh python từ thư mục gốc của dự án.
import utils.cassandra_connector as cassandra_db

app = Flask(__name__)

# --- Khởi động ---
# Tự động chạy setup (tạo keyspace, table) khi web app khởi động
# Hàm `get_cassandra_session` có retry tích hợp,
# nên nó sẽ chờ Cassandra sẵn sàng.
print("Initializing Cassandra...")
try:
    # 1. GỌI HÀM CASSANDRA (không phải hbase)
    cassandra_db.create_keyspace_and_table()
    print("Cassandra initialization complete.")
except Exception as e:
    print(f"Cassandra initialization FAILED: {e}", file=sys.stderr)

# --- Các route của Web ---
@app.route('/', methods=['GET', 'POST'])
def index():
    recommendations = []
    user_id = ""
    error_message = ""
    
    if request.method == 'POST':
        # 2. Đảm bảo 'user_id' khớp với file index.html
        user_id = request.form.get('userId', '').strip() 
        
        if user_id:
            print(f"Fetching recommendations for: {user_id}")
            try:
                # 3. GỌI HÀM CASSANDRA
                recs_list = cassandra_db.read_recs(user_id)
                
                if recs_list:
                    recommendations = recs_list
                else:
                    error_message = f"No recommendations found for user '{user_id}'."
            except Exception as e:
                print(f"Error in web app: {e}")
                # 4. Sửa thông báo lỗi
                error_message = "An error occurred while fetching data from Cassandra." 
        else:
            error_message = "Please enter a User ID."

    # Render file 'index.html'
    return render_template(
        'index.html', 
        user_id=user_id, 
        recommendations=recommendations,
        error_message=error_message
    )

if __name__ == '__main__':
    # Chạy ở cổng 5000, mở cho tất cả IP trong mạng docker
    app.run(debug=True, host='0.0.0.0', port=5000)