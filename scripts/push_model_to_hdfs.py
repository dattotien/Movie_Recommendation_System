import sys
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

# --- CẤU HÌNH ---
# 
# ⚠️ QUAN TRỌNG: 
# HÃY SỬA ĐƯỜNG DẪN NÀY
# Trỏ đến thư mục model BẠN ĐÃ HUẤN LUYỆN (nằm bên trong container /app)
#
# Ví dụ: /app/notebooks/my_als_model_directory
# 
# SỬA 1: Đã cập nhật đường dẫn tới model của bạn
LOCAL_MODEL_PATH = "file:///app/src/batch/als_model_32m"

# Nơi Lớp Speed (của bạn) đang tìm model
HDFS_MODEL_PATH = "hdfs://namenode:9000/user/hadoop/als_model"

def push_model_to_hdfs():
    """
    Tải model đã huấn luyện từ thư mục local 
    và lưu (ghi đè) lên HDFS.
    """
    print("--- Bắt đầu job ĐẨY MODEL LÊN HDFS ---")
    
    spark = SparkSession.builder \
        .appName("ModelPusher") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Tải model từ thư mục local
    print(f"Đang tải model local từ: {LOCAL_MODEL_PATH}")
    try:
        model = ALSModel.load(LOCAL_MODEL_PATH)
        print("✅ Tải model local thành công.")
    except Exception as e:
        print(f"❌ LỖI: Không tìm thấy model local tại '{LOCAL_MODEL_PATH}'")
        print("👉 Hãy chắc chắn thư mục model của bạn nằm ở 'src/batch/als_model_32m'.")
        print(f"Lỗi chi tiết: {e}")
        spark.stop()
        sys.exit(1)

    # 2. Đẩy model lên HDFS
    try:
        print(f"Đang đẩy model lên HDFS tại: {HDFS_MODEL_PATH}")
        # Ghi đè nếu model cũ tồn tại
        model.write().overwrite().save(HDFS_MODEL_PATH)
        print("✅ Đã lưu model lên HDFS thành công!")
        print("--- Job ĐẨY MODEL hoàn tất ---")
        
    except Exception as e:
        print(f"❌ LỖI: Không thể LƯU model vào HDFS.")
        print(f"👉 Hãy chắc chắn HDFS (namenode, datanode) đang chạy.")
        print(f"Lỗi chi tiết: {e}")
    
    spark.stop()

if __name__ == "__main__":
    # SỬA 2: Chạy trực tiếp
    push_model_to_hdfs()