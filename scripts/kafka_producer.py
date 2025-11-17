import csv
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Các hằng số cấu hình ---
KAFKA_TOPIC = 'new_ratings'
KAFKA_SERVER = 'kafka:9092' 

# SỬA 1: Đường dẫn file. Chúng ta dùng 32M như log của bạn
DATA_FILE_PATH = '/app/data/ml-32m/ml-32m/ratings.csv' 
SLEEP_TIME = 0.5 

# SỬA 2: Định nghĩa Producer toàn cục (global) để tái sử dụng khi được import
_producer = None

def create_producer():
    """Khởi tạo Kafka Producer với cơ chế thử lại (retry) và lưu vào biến global."""
    global _producer
    if _producer is not None:
        return _producer # Trả về producer đã có

    print(f"Đang kết nối tới Kafka Server: {KAFKA_SERVER}...")
    
    retries = 5
    while retries > 0:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: str(v).encode('utf-8') 
            )
            print("✅ Đã kết nối Kafka thành công!")
            return _producer
        except NoBrokersAvailable:
            print(f"Không thể kết nối Kafka. Đang thử lại sau 5 giây... (còn {retries} lần)", file=sys.stderr)
            retries -= 1
            time.sleep(5)
    
    print("❌ Lỗi: Không thể kết nối tới Kafka sau nhiều lần thử.", file=sys.stderr)
    return None

def send_new_rating(user_id: int, movie_id: int, rating: float):
    """
    SỬA 3: Hàm này được Web App (app.py) import để gửi 1 rating đơn lẻ.
    """
    producer = create_producer()
    if producer is None:
        print("❌ Không thể gửi rating, Kafka Producer không khả dụng.", file=sys.stderr)
        return

    # Tạo message (dạng chuỗi, phân cách bằng dấu phẩy)
    # Đảm bảo user_id, movie_id, rating là chuỗi để gửi.
    message = f"{user_id},{movie_id},{rating}"
    
    try:
        producer.send(KAFKA_TOPIC, value=message)
        producer.flush() # Bắt buộc phải flush để gửi ngay lập tức
        print(f"✅ Gửi thành công rating mới từ Web App: {message}")
    except Exception as e:
        print(f"❌ Lỗi khi gửi rating: {e}", file=sys.stderr)


def stream_data(producer):
    """
    Đọc file ratings.csv và stream dữ liệu vào Kafka.
    """
    
    print(f"Bắt đầu đọc dữ liệu từ file: {DATA_FILE_PATH}")
    try:
        with open(DATA_FILE_PATH, mode='r', encoding='latin-1') as file:
            reader = csv.reader(file, delimiter=',')
            
            header = next(reader)
            print(f"Bỏ qua header: {header}")

            print(f"Bắt đầu stream dữ liệu vào topic '{KAFKA_TOPIC}'...")
            
            i = 0
            for row in reader:
                if len(row) < 3:
                    continue
                
                # Tạo message (dạng chuỗi, phân cách bằng dấu phẩy)
                message = f"{row[0]},{row[1]},{row[2]}"
                
                # Gửi message vào Kafka
                producer.send(KAFKA_TOPIC, value=message)
                
                i += 1
                if (i % 100) == 0:
                    print(f"Đã gửi {i} ratings. Rating cuối: {message}")
                
                time.sleep(SLEEP_TIME)

            producer.flush() 
            print(f"✅ Đã gửi toàn bộ dữ liệu ({i} ratings).")

    except FileNotFoundError:
        print(f"❌ LỖI: Không tìm thấy file data tại {DATA_FILE_PATH}", file=sys.stderr)
        print("👉 Gợi ý: Hãy kiểm tra kỹ đường dẫn và cấu trúc thư mục data/", file=sys.stderr)
    except Exception as e:
        print(f"❌ Lỗi trong quá trình stream: {e}", e.__class__.__name__, file=sys.stderr)

# --- Hàm main để chạy script ---
if __name__ == "__main__":
    producer = create_producer()
    
    if producer:
        try:
            stream_data(producer)
        except KeyboardInterrupt:
            print("\nĐã dừng stream (nhận lệnh Ctrl+C).")
        finally:
            print("Đóng kết nối Kafka.")
            producer.close()