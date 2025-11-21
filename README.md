# Movie_Recommendation_System

## Table of Contents
- [1. Project Overview](#1-project-overview)
- [2. Use Case & Industry Background](#2-use-case--industry-background)
- [3. Dataset Overview](#3-dataset-overview)
- [4. System Architecture](#4-system-architecture)
- [5. Workflow Explanation](#5-workflow-explanation)
  - [Batch Processing](#batch-processing)
  - [Speed Processing](#speed-processing)
- [6. Repository Structure](#6-repository-structure)
- [7. Installation & Requirements](#7-installation--requirements)
- [8. How to Run the Project](#8-how-to-run-the-project)
- [9. Team Responsibilities](#9-team-responsibilities)
- [10. Acknowledgments](#10-acknowledgments)

---

# 1. Project Overview
This project is a **Movie Recommendation System** built entirely using **Big Data technologies**, designed under the **Lambda Architecture**.  
It combines:

- **Batch Layer** → Train ALS model on 32M MovieLens ratings (Spark MLlib)
- **Speed Layer** → Real-time recommendation updates via Kafka + Spark Streaming
- **Serving Layer** → Cassandra as low-latency data store
- **Web Demo** → Flask/Streamlit app for querying recommendations

The system outputs **Top-10 personalized movie recommendations** for every user, updated instantly when new ratings arrive.

---

# 2. Use Case & Industry Background
### **Use Case: Product/Content Recommendation Engine**
Recommendation systems are used by:
- Netflix  
- YouTube  
- Shopee  
- Spotify  
- TikTok  

### **Industries**
- E-commerce  
- Media Streaming  
- Social Networks  

### Big Data Frameworks Used
| Framework | Purpose |
|----------|----------|
| **Apache Spark** | Batch ETL + ALS model training |
| **Apache Kafka** | Ingest new user ratings in real-time |
| **Apache Flink** | (Optional extension) Real-time processing |
| **Hadoop MapReduce** | Large-scale offline preprocessing |
| **HDFS** | Distributed storage |
| **Cassandra** | Low-latency storage for recommendations |

---

# 3. Dataset Overview
📌 **Dataset: MovieLens 32M**  
📌 **Link:** https://www.kaggle.com/datasets/dattotien/bigdata-movies  

### Content
| File | Description |
|------|-------------|
| `ratings.csv` | 32M ratings (userId, movieId, rating, timestamp) |
| `movies.csv` | Movie details (movieId, title, genres) |
| `tags.csv` | 2M user-generated tags |
| `links.csv` | Mapping to IMDb, TMDB, MovieLens |

The dataset includes:
- ⭐ **32,000,000 ratings**
- 🎬 **87,585 movies**
- 👤 **200,000+ users**
- 🏷️ **2,000,000 tags**

Two versions included:
- `ml-1m` → For testing & debugging  
- `ml-32m` → Full-scale final processing  

---

# 4. System Architecture

The project implements the **Lambda Architecture** with 3 primary, complementary layers:

- **Batch Layer (HDFS, Spark MLlib):** Computes high-accuracy "Batch Views" (full historical model retraining).
- **Speed Layer (Kafka, Spark Streaming):** Processes new data streams to generate instant "Real-time Views".
- **Serving Layer (Cassandra):** Merges results from both layers, ensuring extremely low query latency.

---

# 5. Workflow Explanation

### Batch Layer (Historical Processing)
1.  **Ingestion:** Historical and accumulated data is stored in **HDFS**.
2.  **Training:** **Spark MLlib** periodically retrains the **ALS** model on the entire dataset.
3.  **Output:** Pre-calculated recommendations are batch-written to **Cassandra** (Batch View).

### Speed Layer (Real-time Processing)
1.  **Event Ingestion:** New user rating actions are pushed into **Apache Kafka**.
2.  **Inference:** **Spark Streaming** loads the ALS model, reads from Kafka, and re-calculates recommendations **only for the interacting user**.
3.  **Update:** The new recommendation list is immediately written to **Cassandra** (Real-time View).

---

# 6. Repository Structure
```
/Movie_Recommendation_System/
├── 📂 docs/
│   ├── SystemArchitecture.png           # System architecture diagram
│   └── FinalReport.docx                 # Final project report
│
├── 📂 data/
│    ├── 📂 ml-1m/                       # MovieLens 1M dataset (for testing)
│    │   ├── ratings.csv
│    │   ├── movies.csv
│    │   └── README.txt
│    │
│    └── 📂 ml-32m/                      # MovieLens 32M dataset (full-scale)
│         ├── ratings.csv
│         ├── movies.csv
│         └── README.txt
│
├── 📂 notebooks/
│   └── 01-EDA-MovieLens.ipynb           # Exploratory data analysis notebook
│
├── 📂 scripts/
│   ├── load_to_hdfs.sh                  # Script to upload data to HDFS
│   ├── kafka_producer.py                # Sends new ratings to Kafka
│   └── setup_env.sh                     # Environment setup script
│
├── 📂 src/
│   ├── 📦 batch/                         # Batch Layer: Offline processing
│   │   ├── __init__.py
│   │   └── train_model.py               # Train ALS recommendation model
│   │
│   ├── 📦 stream/                        # Speed Layer: Real-time processing
│   │   ├── __init__.py
│   │   └── process_stream.py            # Handle real-time rating ingestion
│   │
│   ├── 📦 webapp/                        # Web application (Flask/Streamlit)
│   │   ├── __init__.py
│   │   ├── app.py                       # Main web interface
│   │   ├── templates/
│   │   │   └── index.html               # UI template
│   │   └── static/
│   │       └── style.css                # Front-end styling
│   │
│   └── 📦 utils/
│       ├── __init__.py
│       └── Cassandra_connector.py       # Cassandra database connector
│
├── .gitignore
├── requirements.txt                     # Required Python dependencies
└── README.md                            # Project documentation
```        


---

# 7. Installation & Requirements
The entire ecosystem is **Dockerized** using `docker-compose.yml`.

**Prerequisites:**
1.  Docker Engine
2.  Docker Compose
3.  Resource Recommendation: Minimum 16GB RAM (required for Spark's In-memory processing).

---

# 8. How to Run the Project (đợi chốt r mới sửa)
1.  **Clone the repository.**
2.  **Start the Cluster:** Execute `docker-compose up -d`.
3.  **Ingest Data:** Run the ingestion script: `./scripts/load_to_hdfs.sh`.
4.  **Run Batch Job:** Execute the Spark Batch training process.
5.  **Run Streaming Job:** Start the Spark Streaming process.
6.  **Run Web App:** Launch the Flask application to query recommendations.

---

# 9. Team Responsibilities

| Name | Responsibilities |
|:---:|:---|
| **Tô Tiến Đạt** | Cassandra Schema Design, Spark-to-Cassandra connection module, Flask Web Application development |
| **Phạm Hà Anh** | Data Analysis (EDA) on MovieLens, Data Ingestion script (ETL to HDFS), **Spark Batch** module development (ALS training) |
| **Chu Thị Phương Anh** | Apache Kafka cluster configuration, **Producer** script for real-time rating stream, **Spark Streaming** module development. |

---

# 10. Acknowledgments
We acknowledge the pioneering work of **Nathan Marz** (Lambda Architecture) and **GroupLens Research** (MovieLens Dataset). Special thanks to Dr. Trần Hồng Việt for guidance.

---
