from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.recommendation import ALSModel

from utils import cassandra_connector as db

MODEL_PATH = "file:///app/src/batch/als_model_32m"  # hoặc HDFS path nếu đã push
TOP_K = 60

def main():
    spark = SparkSession.builder.appName("WriteBatchRecs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Đang load model từ {MODEL_PATH}")
    model = ALSModel.load(MODEL_PATH)

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