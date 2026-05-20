# Netflix Data Engineering Project 🚀

## 📌 Project Overview

This project demonstrates an AWS Data Engineering pipeline using the Netflix dataset.

The project includes:
- Data ingestion
- ETL processing
- Data cleaning
- Medallion architecture implementation

---

# 🏗️ Architecture

Raw Data (Bronze)
        ↓
AWS Glue ETL
        ↓
Cleaned Data (Silver)
        ↓
Business Data (Gold)

---

# ⚙️ AWS Services Used

| Service | Purpose |
|----------|----------|
| Amazon S3 | Store raw and processed data |
| AWS Glue | ETL processing |
| AWS Lambda | Trigger Glue jobs |
| AWS Step Functions | Workflow orchestration |

---

# 📂 Project Structure

```text
Netflix-Data-Engineering-Project/
│
├── scripts/
│
├── glue/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── lambda/
│
├── stepfunctions/
│
├── sql/
│
├── screenshots/
│
└── README.md
```

---

# 🔄 Workflow

1. Upload Netflix dataset to S3 Bronze layer
2. AWS Glue cleans raw data
3. Cleaned data stored in Silver layer
4. Aggregated data stored in Gold layer
5. Lambda triggers ETL workflow
6. Step Functions manages pipeline execution

---

# 🧠 Medallion Architecture

## 🥉 Bronze Layer
Stores raw Netflix CSV data.

## 🥈 Silver Layer
Stores cleaned and transformed data.

## 🥇 Gold Layer
Stores business-ready transformed data.

---

# 🚀 Key Features

- End-to-end AWS ETL pipeline
- Serverless architecture
- Automated workflow
- Scalable cloud processing
- Medallion architecture design

---

# 💼 Resume Highlights

- Built a scalable AWS data engineering pipeline
- Implemented Bronze-Silver-Gold architecture
- Automated ETL workflows using AWS Glue
- Developed serverless data processing workflow

---

# 👨‍💻 Author

Gangadharan D

AWS | Data Engineering | Python | SQL