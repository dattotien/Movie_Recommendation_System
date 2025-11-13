from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf
from pyspark.sql.types import ArrayType, StringType
import time

# --- PHẦN GIẢ LẬP (MOCK) ---

# 1. Giả lập Task của Người 2 (Model)
# Thay vì tải mô hình ALS, chúng ta tạo một hàm "giả"
@udf(returnType=ArrayType(StringType()))
def get_mock_recommendations(user_id):
    """
    Hàm giả lập việc tính toán Top 10.
    Luôn trả về 2 phim cố định, bất kể user là ai.
    """
    print(f"--- [MOCK MODEL] Đang tính toán giả cho user {user_id} ---")
    
    # (Khi Người 2 làm xong, chúng ta sẽ thay thế hàm này
    # bằng code tải mô hình ALS và .recommendForUserSubset)
    
    # Trả về 1 danh sách phim "giả"
    return [f"mock_movie_for_{user_id}_1", "mock_movie_2"]

# 2. Giả lập Task của Người 3 (Database Connector)
# Chúng ta viết một hàm "giả" thay vì 'import'
def write_recs_mock(user_id, recommendations_list):
    """
    Hàm giả lập việc ghi vào HBase/Cassandra.
    Chỉ in ra Terminal thay vì ghi vào DB thật.
    """
    
    # (Khi Người 3 làm xong, chúng ta sẽ 'import' code của họ
    # và gọi: connector.write(user_id, recommendations_list))
    
    print(f"--- [MOCK DB] GHI ĐÈ CHO USER {user_id}: {recommendations_list} ---")
    return True

# --- HÀM XỬ LÝ STREAM (foreachBatch) ---

def process_batch(batch_df, batch_id):
    """
    Hàm này được gọi mỗi khi Spark có một "batch" (lô) dữ liệu mới từ Kafka.
    """
    print(f"\nĐang xử lý Batch ID: {batch_id}")
    
    # Kiểm tra xem batch có dữ liệu không
    if batch_df.count() > 0:
        # 1. Tính toán gợi ý "giả"
        # Áp dụng hàm "giả" vào cột 'userId'
        # Dùng .distinct() để tránh tính toán nhiều lần cho cùng 1 user trong 1 batch
        distinct_users_df = batch_df.select("userId").distinct()
        
        recs_df = distinct_users_df.withColumn("recommendations", get_mock_recommendations(col("userId")))
        
        # 2. Thu thập kết quả về máy (vì batch nhỏ)
        results = recs_df.collect()
        
        # 3. Ghi "giả" vào DB
        if results:
            print(f"Batch {batch_id} có {len(results)} users để cập nhật:")
            for row in results:
                user_id = row['userId']
                recs_list = row['recommendations']
                
                # Gọi hàm ghi "giả"
                write_recs_mock(user_id, recs_list)
        else:
            print(f"Batch {batch_id} không có kết quả (có thể đã xử lý).")
    else:
        print(f"Batch {batch_id} không có data.")

# --- HÀM MAIN ---

def main():
    print("Khởi động job Spark Streaming (Task 1.2 - Giai đoạn B Mocking)...")
    
    spark = SparkSession.builder \
        .appName("SpeedLayerProcessor_Mocked") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN") # Giảm log rác

    # 1. Đọc (Read) từ Kafka
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "new_ratings") \
      .load()
    
    print("Đã kết nối Kafka, đang lắng nghe topic 'new_ratings'...")

    # 2. Xử lý (Parse) message "userId,movieId,rating"
    ratings_df = df.selectExpr("CAST(value AS STRING)") \
        .select(
            split(col("value"), ",")[0].alias("userId"),
            split(col("value"), ",")[1].alias("movieId"),
            split(col("value"), ",")[2].alias("rating")
        )

    # 3. Ghi (Write) bằng hàm 'foreachBatch'
    # Dùng trigger processingTime='15 seconds' để gom data mỗi 15 giây
    query = ratings_df \
      .writeStream \
      .trigger(processingTime='15 seconds') \
      .outputMode("update") \
      .foreachBatch(process_batch) \
      .start()
      
    print("Đã khởi động query, đang chờ data từ Task 1.1...")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()