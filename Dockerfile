FROM python:3.9-slim-bullseye

RUN apt-get update && \
    apt-get install -y \
        openjdk-11-jdk-headless \
        wget \
        curl \
        openssh-client \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz && \
    mv hadoop-3.2.1 /opt/hadoop && \
    rm hadoop-3.2.1.tar.gz

# Tạo thư mục cấu hình
RUN mkdir -p /opt/hadoop/etc/hadoop

# Tạo file core-site.xml bằng echo (cách đúng)
RUN echo '<?xml version="1.0" encoding="UTF-8"?>' > /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '<configuration>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '    <property>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '        <name>fs.defaultFS</name>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '        <value>hdfs://namenode:9000</value>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '    </property>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '    <property>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '        <name>hadoop.tmp.dir</name>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '        <value>/tmp/hadoop</value>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '    </property>' >> /opt/hadoop/etc/hadoop/core-site.xml && \
    echo '</configuration>' >> /opt/hadoop/etc/hadoop/core-site.xml

# Tạo file hdfs-site.xml bằng echo
RUN echo '<?xml version="1.0" encoding="UTF-8"?>' > /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    echo '<configuration>' >> /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    echo '    <property>' >> /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    echo '        <name>dfs.replication</name>' >> /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    echo '        <value>1</value>' >> /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    echo '    </property>' >> /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    echo '</configuration>' >> /opt/hadoop/etc/hadoop/hdfs-site.xml


ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV HADOOP_COMMON_HOME=/opt/hadoop
ENV HADOOP_HDFS_HOME=/opt/hadoop
ENV HADOOP_MAPRED_HOME=/opt/hadoop
ENV HADOOP_YARN_HOME=/opt/hadoop
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data /app/scripts



EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]