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

The data pipeline is designed using a fully serverless, event-driven architecture on AWS, ensuring scalability, efficiency, and automation throughout the data lifecycle. 

Here’s how the workflow operates:

![Pipeline Architecture](pipeline_architecture.svg)

---

## Tech Stack 🧰


| Category               | Tools & Services                         |
|------------------------|------------------------------------------|
| Programming Language   | Python                                   |
| AWS Services           | Lambda, CloudWatch, EventBridge, SQS, S3, DynamoDB, Parameter Store, Secrets Manager |
| Database               | MongoDB Atlas                            |
| Infrastructure as Code | AWS CDK (Python)                         |
| Data Publishing        | Kaggle API                               |
| Notifications          | Telegram Bot API                         |
| Documentation          | draw.io (diagrams.net)                   |

---

## 🧱 Infrastructure as Code (IaC)

This project embraces the practice of **Infrastructure as Code (IaC)** philosophy using **[AWS CDK](https://docs.aws.amazon.com/cdk/)** (in Python) to provision and manage cloud resources.


### 💡 Advantages of leveraging IaC


- **Version Control**: All infrastructure is declared in code and tracked in Git.
- **Centralized Control**: All AWS resources are organized and deployed under a **single CDK stack**, making them easier to maintain, modify, and tear down.
- **Automation**: No manual clicks in the AWS console—deployment is fully automated.

### 🧰 Resources Defined via CDK

With AWS CDK, the following resources are created and configured programmatically:

| Resource Type          | Purpose                                                |
|------------------------|--------------------------------------------------------|
| 🪣 **S3 Buckets**        | For storing downloaded and processed files            |
| 🧮 **DynamoDB Table**    | To track processed match files                        |
| 🧠 **Lambda Functions**  | For each pipeline task (download, extract, transform, upload) |
| 🔁 **SQS & EventBridge** | To trigger Lambdas asynchronously                    |
| 🔐 **IAM Roles**         | With scoped permissions for security                 |
| 🧾 **SSM Parameters**    | For storing API keys, tokens, and config             |
| 📆 **CloudWatch Schedulers** | To run jobs on a weekly basis                     |


---
## 📦 Code Packaging

For every code changes, this project is leveraing `build_packages` and `cdk deploy` commands to package and deploy the code respectively.

### 🛠  Lambda Packaging Utility (`src\build\build_packages.py`) 

This utility script automates the packaging process for both:

- 📦 **AWS Lambda Layers** (for dependencies like `pymongo`, `kaggle`, `requests`)
- 🧾 **Lambda Handler Zips** (each respective Lambda function code files)

#### Purposes

The `build_packages.py` script streamlines the deployment workflow by:

1. **Building a Lambda Layer:**
   - Creates a source distribution (`.tar.gz`) using your `setup.py`
   - Extracts it into a `site-packages` directory
   - Installs dependencies listed in `requirements.txt` using a Lambda-compatible environment
   - Zips the `site-packages` directory for deployment as an AWS Lambda Layer

2. **Zipping Lambda Code:**
   - Each handler file is compressed into its own zip archive, ready for deployment

3. **Cleaning Up:**
   - Removes temporary folders and tarballs to keep your workspace clean

---

## 🚀 Deployment
After packages, all the resources will get deployed with the CDK command 

```bash
cdk deploy
```

- This command deploys S3 buckets, Lambda functions, DynamoDb tables, EventBridge rules, SQS queues, IAM roles, SSM parameters and CloudWatch schedulers.

- Everything is deployed in one go via a single CDK stack, making the infrastructure highly repeatable and version-controlled.

---

## 🤝 Contribution

This project is a solo build by me, but if you'd like to raise issues, fork, or explore, feel free to open a discussion or submit a pull request.

## 📄 License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

©<a href="https://github.com/NishanthMuruganantham">Nishanth</a>

---