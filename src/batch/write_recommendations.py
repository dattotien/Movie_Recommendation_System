import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.recommendation import ALSModel

from utils import cassandra_connector as db

LOCAL_MODEL_PATH = os.environ.get("ALS_MODEL_LOCAL_PATH", "file:///app/src/batch/als_model_32m")
HDFS_MODEL_PATH = os.environ.get("ALS_MODEL_HDFS_PATH", "hdfs://namenode:9000/user/hadoop/als_model")
MODEL_LOAD_STRATEGY = os.environ.get("ALS_MODEL_LOAD_STRATEGY", "local-first").lower()  # local-first|hdfs-first|hdfs-only
AUTO_PUSH_MODEL_TO_HDFS = os.environ.get("ALS_MODEL_AUTO_PUSH", "true").lower() == "true"
ITEM_FACTORS_EXPORT_PATH = os.environ.get("ALS_ITEM_FACTORS_PATH", "hdfs://namenode:9000/user/hadoop/als_item_factors")
AUX_EXPORT_ITEM_FACTORS = os.environ.get("ALS_EXPORT_ITEM_FACTORS", "true").lower() == "true"
TOP_K = int(os.environ.get("ALS_TOP_K", "30"))


def load_model(spark: SparkSession) -> ALSModel:
    strategies = {
        "local-first": ["local", "hdfs"],
        "hdfs-first": ["hdfs", "local"],
        "hdfs-only": ["hdfs"],
    }
    order = strategies.get(MODEL_LOAD_STRATEGY, ["local", "hdfs"])
    last_error = None

    for source in order:
        path = LOCAL_MODEL_PATH if source == "local" else HDFS_MODEL_PATH
        try:
            print(f" Đang tải model ALS từ ({source.upper()}): {path}")
            model = ALSModel.load(path)
            print("Tải model thành công.")

            if source == "local" and AUTO_PUSH_MODEL_TO_HDFS:
                try:
                    print(f"Mirror model local lên HDFS: {HDFS_MODEL_PATH}")
                    model.write().overwrite().save(HDFS_MODEL_PATH)
                    print("Đã cập nhật model trên HDFS.")
                except Exception as push_err:
                    print(f" Không thể push model lên HDFS: {push_err}")

            if AUX_EXPORT_ITEM_FACTORS and ITEM_FACTORS_EXPORT_PATH:
                try:
                    print(f"Đang export itemFactors tới {ITEM_FACTORS_EXPORT_PATH} ...")
                    model.itemFactors.write.mode("overwrite").parquet(ITEM_FACTORS_EXPORT_PATH)
                    print("Export itemFactors thành công.")
                except Exception as export_err:
                    print(f"Không thể export itemFactors: {export_err}")
            return model
        except Exception as err:
            print(f"Không load được model từ {path}: {err}")
            last_error = err

    raise RuntimeError(
        f"Không thể load model ALS. Chiến lược đã thử: {order}. "
        f"Kiểm tra lại đường dẫn LOCAL_MODEL_PATH/HDFS_MODEL_PATH. Lỗi cuối: {last_error}"
    )


def main():
    spark = SparkSession.builder.appName("WriteBatchRecs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    model = load_model(spark)

    print(f"Tính recommendForAllUsers({TOP_K}) ...")
    recs_df = model.recommendForAllUsers(TOP_K) \
        .select(col("userId"),
                col("recommendations.movieId").alias("movies"))

    db.create_keyspace_and_table()
    session = db.get_cassandra_session()
    if session is None:
        raise RuntimeError("Không kết nối được Cassandra")

    total = 0
    for row in recs_df.toLocalIterator():
        db.write_recs(session, str(row.userId), row.movies)
        total += 1
        if total % 1000 == 0:
            print(f"Đã ghi {total} users...")

    print(f"Hoàn tất, đã ghi {total} users.")
    spark.stop()


if __name__ == "__main__":
    main()