import csv
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Các hằng số cấu hình ---
KAFKA_TOPIC = 'new_ratings'
KAFKA_SERVER = 'kafka:9092' 

# 1. SỬA ĐƯỜNG DẪN: Trỏ đến file .dat
DATA_FILE_PATH = '/app/data/ml-1m/ml-1m/ratings.dat' 
SLEEP_TIME = 0.5 

def create_producer():
    """Khởi tạo Kafka Producer với cơ chế thử lại (retry)"""
    print("Đang kết nối tới Kafka...")
    producer = None
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: str(v).encode('utf-8') 
            )
            print("Đã kết nối Kafka thành công!")
            return producer
        except NoBrokersAvailable:
            print(f"Không thể kết nối Kafka. Đang thử lại sau 5 giây... (còn {retries} lần)")
            retries -= 1
            time.sleep(5)
    
    print("Lỗi: Không thể kết nối tới Kafka sau nhiều lần thử.")
    return None

def stream_data(producer):
    """Đọc file .dat (dùng '::') và gửi từng dòng vào Kafka"""
    
    # Đường dẫn này đã ĐÚNG
    DATA_FILE_PATH = '/app/data/ml-1m/ml-1m/ratings.dat' 
    
    print(f"Bắt đầu đọc dữ liệu từ file: {DATA_FILE_PATH}")
    try:
        # Mở file .dat
        with open(DATA_FILE_PATH, mode='r', encoding='latin-1') as file:
            
            print(f"Bắt đầu stream dữ liệu vào topic '{KAFKA_TOPIC}'...")
            
            i = 0
            # Đọc từng dòng bằng tay
            while True:
                line = file.readline()
                if not line: # Hết file
                    break
                
                # Bỏ qua dòng trống (nếu có)
                line = line.strip()
                if not line:
                    continue

                # Tự tách (split) bằng '::'
                row = line.split('::')
                
                # File .dat có 4 cột: UserID::MovieID::Rating::Timestamp
                if len(row) < 3:
                    continue
                
                userId = row[0]
                movieId = row[1]
                rating = row[2]
                
                # Tạo message (dạng chuỗi, phân cách bằng dấu phẩy)
                message = f"{userId},{movieId},{rating}"
                
                # Gửi message vào Kafka
                producer.send(KAFKA_TOPIC, value=message)
                
                i += 1
                if (i % 100) == 0:
                    print(f"Đã gửi {i} ratings. Rating cuối: {message}")
                
                time.sleep(SLEEP_TIME)

            producer.flush() 
            print(f"Đã gửi toàn bộ dữ liệu ({i} ratings).")

    except FileNotFoundError:
        print(f"LỖI: Không tìm thấy file data tại {DATA_FILE_PATH}")
    except Exception as e:
        print(f"Lỗi trong quá trình stream: {e}", e.__class__.__name__)

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