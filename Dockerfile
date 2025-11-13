# SỬA LỖI: Dùng "bullseye" (Debian 11) thay vì bản "latest" (Debian 13)
FROM python:3.9-slim-bullseye

# Bây giờ lệnh này SẼ THÀNH CÔNG
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Đặt JAVA_HOME (Rất quan trọng)
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# (Phần còn lại giữ nguyên)
WORKDIR /app
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]