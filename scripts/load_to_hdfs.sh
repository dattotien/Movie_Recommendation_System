# Cấu hình Hadoop
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

mkdir -p $HADOOP_CONF_DIR

cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
</configuration>
EOF

HDFS_BASE="/movielens"

DATA_1M="/app/data/ml-1m"    
DATA_32M="/app/data/ml-32m"  

echo "===== Kiểm tra môi trường ====="
java -version
echo "HADOOP_HOME: $HADOOP_HOME"

echo "===== Kiểm tra kết nối HDFS ====="
hdfs dfsadmin -report

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