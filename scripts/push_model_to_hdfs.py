import sys
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

LOCAL_MODEL_PATH = "file:///app/src/batch/als_model_32m"

HDFS_MODEL_PATH = "hdfs://namenode:9000/user/hadoop/als_model"

def push_model_to_hdfs():
    print("--- Bắt đầu job ĐẨY MODEL LÊN HDFS ---")
    
    spark = SparkSession.builder \
        .appName("ModelPusher") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Đang tải model local từ: {LOCAL_MODEL_PATH}")
    try:
        model = ALSModel.load(LOCAL_MODEL_PATH)
        print("Tải model local thành công.")
    except Exception as e:
        print(f"LỖI: Không tìm thấy model local tại '{LOCAL_MODEL_PATH}'")
        print(f"Lỗi chi tiết: {e}")
        spark.stop()
        sys.exit(1)

    # 2. Đẩy model lên HDFS
    try:
        print(f"Đang đẩy model lên HDFS tại: {HDFS_MODEL_PATH}")
        model.write().overwrite().save(HDFS_MODEL_PATH)
        print("Đã lưu model lên HDFS thành công!")
        print("--- Job ĐẨY MODEL hoàn tất ---")
        
    except Exception as e:
        print(f"LỖI: Không thể LƯU model vào HDFS.")
        print(f"Lỗi chi tiết: {e}")
    
    spark.stop()

if __name__ == "__main__":
    push_model_to_hdfs()