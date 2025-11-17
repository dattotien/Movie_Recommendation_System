from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel
import time
import sys
from collections import defaultdict

# --- PHẦN 1: THAY THẾ MOCK BẰNG IMPORT THẬT ---

# 1.1. Import thư viện của Người 3 (Cassandra Connector)
try:
    # PYTHONPATH đã được set trong docker-compose.yml
    import utils.cassandra_connector as db_connector
    print("✅ Đã import Cassandra_connector thành công!")
except ModuleNotFoundError:
    print("❌ LỖI: Không tìm thấy file 'utils/Cassandra_connector.py'.")
    print("👉 Đảm bảo Người 3 đã hoàn thành Task 3.2.")
    sys.exit(1)


# 1.2. Biến Global cho Model ALS (Task của Người 2)
# Biến này sẽ giữ model ALS đã huấn luyện để không phải tải lại mỗi batch
global_als_model = None

HDFS_MODEL_PATH = "hdfs://namenode:9000/user/hadoop/als_model" 

def get_real_model():
    """
    Hàm này tải model ALS từ HDFS (nếu chưa tải) và lưu vào biến global.
    Việc này đảm bảo chúng ta chỉ tải model 1 LẦN.
    """
    global global_als_model
    if global_als_model is None:
        print(f"--- [REAL MODEL] Lần đầu, đang tải mô hình ALS từ: {HDFS_MODEL_PATH} ---")
        try:
            # Tải model ALS mà Người 2 đã huấn luyện
            global_als_model = ALSModel.load(HDFS_MODEL_PATH)
            print("✅ Tải mô hình ALS thành công!")
        except Exception as e:
            print(f"❌ LỖI NGHIÊM TRỌNG: Không thể tải model ALS từ HDFS.")
            print(f"👉 Hãy chắc chắn Người 2 (Hà Anh) đã chạy 'train_model.py' thành công.")
            print(f"Lỗi chi tiết: {e}")
            sys.exit(1) # Dừng script nếu không tải được model
            
    return global_als_model

# --- PHẦN 2: HÀM XỬ LÝ STREAM (foreachBatch) ĐÃ NÂNG CẤP ---

def process_batch(batch_df, batch_id):
    """
    Hàm này được gọi mỗi khi Spark có một "batch" (lô) dữ liệu mới từ Kafka.
    """
    print(f"\nĐang xử lý Batch ID: {batch_id}")
    
    batch_count = batch_df.count()
    if batch_count > 0:
        print(f"Batch {batch_id} có {batch_count} ratings mới.")
        
        # 1. Thu thập thông tin ratings mới: lưu những phim user đã rate thấp (<= 2.0)
        low_rated_movies = defaultdict(set)  # {userId: {movieId1, movieId2, ...}}
        ratings_data = batch_df.select(
            col("userId").cast(IntegerType()).alias("userId"),
            col("movieId").cast(IntegerType()).alias("movieId"),
            col("rating").cast(FloatType()).alias("rating")
        ).collect()
        
        for rating_row in ratings_data:
            user_id = rating_row['userId']
            movie_id = rating_row['movieId']
            rating_value = rating_row['rating']
            if rating_value <= 2.0:  # Ngưỡng: rate <= 2.0 là "thấp"
                low_rated_movies[user_id].add(movie_id)
                print(f"  → User {user_id} đã rate thấp phim {movie_id} (rating={rating_value})")
        
        # 2. Lấy danh sách user
        distinct_users_df = batch_df.select(col("userId").cast(IntegerType())).distinct()
        
        # 3. Tải model ALS (từ cache global)
        model = get_real_model()
        if model is None:
            print("Model ALS chưa được tải. Bỏ qua batch.")
            return

        # 4. Tính toán Top 10 gợi ý
        print(f"--- [REAL MODEL] Đang tính toán Top 10 cho {distinct_users_df.count()} user... ---")
        recs_df = model.recommendForUserSubset(distinct_users_df, 60)
        
        # 5. Thu thập kết quả
        results = recs_df.select("userId", col("recommendations.movieId").alias("movies_list")).collect()
        
        if results:
            print(f"Batch {batch_id} có {len(results)} users để cập nhật vào Cassandra:")
            
            # --- PHẦN SỬA LỖI QUAN TRỌNG ---
            
            # 6. LẤY SESSION 1 LẦN DUY NHẤT (ở ngoài vòng lặp)
            #    Sử dụng hàm get_cassandra_session() mà Người 3 đã viết
            db_connector.create_keyspace_and_table()
            session = db_connector.get_cassandra_session()
            
            if session is None:
                print("❌ Không thể lấy session Cassandra. Bỏ qua ghi vào DB.")
                return

            # 7. Ghi từng user vào Cassandra (TRUYỀN session vào)
            for row in results:
                user_id = row['userId']
                recs_list = row['movies_list']
                
                # LỌC: Loại bỏ những phim user đã rate thấp trong batch này
                if user_id in low_rated_movies:
                    excluded_movies = low_rated_movies[user_id]
                    recs_list_filtered = [movie_id for movie_id in recs_list if movie_id not in excluded_movies]
                    if len(recs_list_filtered) < len(recs_list):
                        print(f"  → User {user_id}: Đã loại bỏ {len(recs_list) - len(recs_list_filtered)} phim rate thấp khỏi top 10")
                    recs_list = recs_list_filtered
                
                # SỬA 1: Truyền session vào
                # Đây chính là bản TỐI ƯU
                db_connector.write_recs(session, str(user_id), recs_list)
            
            # Lưu ý: Không shutdown session ở đây, vì nó là global
            print(f"--- [REAL DB] Đã ghi xong {len(results)} users vào Cassandra (ĐÃ TỐI ƯU) ---")
            
        else:
            print(f"Batch {batch_id} không có kết quả (lỗi tính toán?).")
    else:
        print(f"Batch {batch_id} không có data mới.")

# --- PHẦN 3: HÀM MAIN (Giữ nguyên) ---

def main():
    print("Khởi động job Spark Streaming (LỚP SPEED - PHIÊN BẢN THẬT)...")
    
    spark = SparkSession.builder \
        .appName("SpeedLayerProcessor_REAL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN") # Giảm log rác

    # 1. Đọc (Read) từ Kafka
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "new_ratings") \
      .option("startingOffsets", "latest") \
      .load()
    
    print("Đã kết nối Kafka, đang lắng nghe topic 'new_ratings'...")

    # 2. Xử lý (Parse) message "userId,movieId,rating"
    ratings_df = df.selectExpr("CAST(value AS STRING)") \
        .select(
            split(col("value"), ",")[0].cast(IntegerType()).alias("userId"),
            split(col("value"), ",")[1].cast(IntegerType()).alias("movieId"),
            split(col("value"), ",")[2].cast(FloatType()).alias("rating")
        )

    # 3. Ghi (Write) bằng hàm 'foreachBatch'
    query = ratings_df \
      .writeStream \
      .trigger(processingTime='15 seconds') \
      .outputMode("update") \
      .foreachBatch(process_batch) \
      .start()
      
    print("Đã khởi động query, đang chờ data...")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()