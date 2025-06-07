# 🏏 T20I Data Pipeline for Kaggle with AWS

![Made with Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Built on AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20SQS-orange)
![MongoDB Atlas](https://img.shields.io/badge/MongoDB-Atlas-green)
![DynamoDB](https://img.shields.io/badge/DynamoDB-NoSQL-informational)
![Kaggle Dataset](https://img.shields.io/badge/Kaggle-T20I%20Cricket%20Dataset-blue)
![CDK](https://img.shields.io/badge/IaC-AWS%20CDK-informational)

This repository automates the end-to-end extraction, processing, and publishing of Men’s T20 International (T20I) cricket data to [Kaggle](https://www.kaggle.com/datasets/nishanthmuruganantha/mens-t20i-cricket-complete-dataset/data), sourced from [Cricsheet](https://cricsheet.org/), using AWS serverless services.

This data pipeline leverages AWS Lambda functions, EventBridge, and SQS to orchestrate an event-driven, fully serverless processing flow. Data is stored in MongoDB Atlas and AWS S3, converted into CSV format, and then automatically uploaded to Kaggle for public access and analysis. 

The dataset is kept current with automated weekly updates, delivering up-to-date and reliable cricket data without manual effort.

All critical steps in the workflow send real-time execution status updates via a Telegram bot.

---
## Pipeline Architecture Overview ⚙️

The data pipeline is designed using a fully serverless, event-driven architecture on AWS, ensuring scalability, efficiency, and automation throughout the data lifecycle. Here’s how the workflow operates:

![Pipeline Architecture](pipeline_architecture.svg)

---