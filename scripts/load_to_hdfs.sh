#!/bin/bash
# =======================================================
# Script: load_to_hdfs.sh - Đã fix JAVA_HOME
# =======================================================
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop

echo "===== Kiểm tra môi trường ====="
java -version
echo "JAVA_HOME: $JAVA_HOME"

# HDFS Configuration
HDFS_BASE="/user/haanh/movielens"

# Local dataset paths
DATA_1M="/mnt/d/BTL/Movie_Recommendation_System/data/ml-1M"
DATA_32M="/mnt/d/BTL/Movie_Recommendation_System/data/ml-32M"


echo "===== Kiểm tra dữ liệu local ====="
echo "ML-1M:"
ls -la $DATA_1M/*.dat
echo "ML-32M:"
ls -la $DATA_32M/*.csv

echo "===== Tạo thư mục HDFS ====="
hdfs dfs -mkdir -p $HDFS_BASE/1M
hdfs dfs -mkdir -p $HDFS_BASE/32M

echo "===== Upload dữ liệu 1M (.dat files) ====="
for file in $DATA_1M/*.dat; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "Uploading $filename..."
        hdfs dfs -put "$file" $HDFS_BASE/1M/
    fi
done

echo "===== Upload dữ liệu 32M (.csv files) ====="
for file in $DATA_32M/*.csv; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "Uploading $filename..."
        hdfs dfs -put "$file" $HDFS_BASE/32M/
    fi
done

echo "===== Kiểm tra dữ liệu trên HDFS ====="
echo "Dữ liệu 1M:"
hdfs dfs -ls $HDFS_BASE/1M/
echo "Dữ liệu 32M:"
hdfs dfs -ls $HDFS_BASE/32M/

echo "===== HOÀN TẤT ====="