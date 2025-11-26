import os
import sys
from collections import defaultdict
from typing import Dict

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel

from online.user_factor_utils import (
    solve_user_factor,
    recommend_from_user_factor,
)


try:
    import utils.cassandra_connector as db_connector
    print(" Đã import Cassandra_connector thành công!")
except ModuleNotFoundError:
    print(" LỖI: Không tìm thấy file 'utils/Cassandra_connector.py'.")
    sys.exit(1)

# --- CẤU HÌNH ---

HDFS_MODEL_PATH = os.environ.get("ALS_MODEL_HDFS_PATH", "hdfs://namenode:9000/user/hadoop/als_model")
ITEM_FACTORS_PATH = os.environ.get("ALS_ITEM_FACTORS_PATH", "hdfs://namenode:9000/user/hadoop/als_item_factors")
ONLINE_USER_REG = float(os.environ.get("ONLINE_USER_REG", "0.1"))
ONLINE_TOP_K = int(os.environ.get("ONLINE_TOP_K", "30"))
FALLBACK_TOP_K = int(os.environ.get("ALS_TOP_K", "30"))

global_als_model = None


def get_real_model():
    global global_als_model
    if global_als_model is None:
        print(f"Đang tải mô hình ALS từ: {HDFS_MODEL_PATH} ---")
        global_als_model = ALSModel.load(HDFS_MODEL_PATH)
        print("Tải mô hình ALS thành công!")
    return global_als_model


def create_process_batch(item_factors_map: Dict[int, np.ndarray]):

    def process_batch(batch_df, batch_id):
        print(f"\nĐang xử lý Batch ID: {batch_id}")
        batch_records = batch_df.collect()

        if not batch_records:
            print(f"Batch {batch_id} không có data mới.")
            return

        session = db_connector.get_cassandra_session()
        if session is None:
            print(" Không thể lấy session Cassandra. Bỏ qua batch.")
            return

        user_events = defaultdict(list)  # userId -> list[(vector, rating, movieId)]
        fallback_user_ids = set()

        for row in batch_records:
            user_id = int(row.userId)
            movie_id = int(row.movieId)
            rating_value = float(row.rating)

            item_vec = item_factors_map.get(movie_id)
            if item_vec is None:
                print(f" Không tìm thấy item factor cho movie {movie_id}.")
                fallback_user_ids.add(user_id)
                continue

            user_events[user_id].append((item_vec, rating_value, movie_id))

        online_updates = 0
        for user_id, entries in user_events.items():
            item_vecs = [vec for vec, _, _ in entries]
            ratings = [rating for _, rating, _ in entries]
            exclude_movies = [movie for _, _, movie in entries]

            try:
                user_vector = solve_user_factor(item_vecs, ratings, ONLINE_USER_REG)
                recs = recommend_from_user_factor(
                    item_factors_map,
                    user_vector,
                    exclude_movies,
                    top_k=ONLINE_TOP_K,
                )
                if recs:
                    db_connector.write_recs(session, str(user_id), recs)
                    online_updates += 1
                    print(f"Đã cập nhật online cho user {user_id}.")
                else:
                    print(f" Không sinh được recs cho user {user_id}, fallback model.")
                    fallback_user_ids.add(user_id)
            except Exception as err:
                print(f"Lỗi khi cập nhật online cho user {user_id}: {err}")
                fallback_user_ids.add(user_id)

        if fallback_user_ids:
            print(f"Fallback ALS cho {len(fallback_user_ids)} users.")
            distinct_users_df = batch_df.filter(col("userId").isin(list(fallback_user_ids))).select(col("userId")).distinct()
            model = get_real_model()
            recs_df = model.recommendForUserSubset(distinct_users_df, FALLBACK_TOP_K)
            results = recs_df.select("userId", col("recommendations.movieId").alias("movies_list")).collect()
            for row in results:
                db_connector.write_recs(session, str(row['userId']), row['movies_list'])
            print(f"--- Đã fallback {len(results)} users ---")

        if online_updates:
            print(f" Batch {batch_id}: cập nhật online thành công {online_updates} users.")

    return process_batch


def main():
    print("Khởi động job Spark Streaming.")

    spark = SparkSession.builder.appName("SpeedLayerProcessor_REAL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Đang load item factors từ {ITEM_FACTORS_PATH} ...")
    item_factors_df = spark.read.parquet(ITEM_FACTORS_PATH).select(
        col("id").cast(IntegerType()).alias("movieId"),
        col("features"),
    )
    item_factors_map = {
        int(row.movieId): np.asarray(row.features, dtype=np.float64)
        for row in item_factors_df.collect()
    }
    print(f"Đã load {len(item_factors_map)} item factors.")

    process_batch_fn = create_process_batch(item_factors_map)

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "new_ratings") \
        .option("startingOffsets", "latest") \
        .load()

    print("Đã kết nối Kafka, đang lắng nghe topic 'new_ratings'...")

    ratings_df = df.selectExpr("CAST(value AS STRING)") \
        .select(
            split(col("value"), ",")[0].cast(IntegerType()).alias("userId"),
            split(col("value"), ",")[1].cast(IntegerType()).alias("movieId"),
            split(col("value"), ",")[2].cast(FloatType()).alias("rating")
        )

    query = ratings_df.writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode("update") \
        .foreachBatch(process_batch_fn) \
        .start()

    print("Đã khởi động query, đang chờ data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()