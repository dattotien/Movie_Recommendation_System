# Đây là nội dung cho file: scripts/kafka_producer.py

import time
import sys

def send_new_rating(user_id, movie_id, rating):
    """
    Hàm GIẢ (mock) để test web app.
    Hàm này không gửi gì đến Kafka, chỉ in ra terminal.
    """
    
    print("="*50, file=sys.stdout)
    print(f"[KAFKA PRODUCER MOCK]: Đã nhận được rating:", file=sys.stdout)
    print(f"[KAFKA PRODUCER MOCK]: User ID:   {user_id}", file=sys.stdout)
    print(f"[KAFKA PRODUCER MOCK]: Movie ID:  {movie_id}", file=sys.stdout)
    print(f"[KAFKA PRODUCER MOCK]: Rating:    {rating}", file=sys.stdout)
    print("="*50, file=sys.stdout)
    
    # Giả vờ có một chút độ trễ
    time.sleep(0.1) 
    
    # Luôn trả về True để báo hiệu "gửi thành công"
    return True

# Bạn cũng có thể thêm các hàm giả khác nếu cần, ví dụ:
# def initialize_producer():
#     print("[KAFKA PRODUCER MOCK]: Đã khởi tạo producer.")
#     return None