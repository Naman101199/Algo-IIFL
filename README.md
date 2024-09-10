# Real-Time Stock Market Data Processing with Apache Kafka, Spark, and AWS

This project captures, processes, and analyzes real-time stock market data using a robust streaming architecture built on Apache Kafka, Apache Spark, Docker, and AWS services.

## Architecture Overview

![image](https://github.com/user-attachments/assets/96206593-0f1c-4533-af70-d0737787d3fb)

### Components:

1. **Data Source**:
    - **IIFL Securities**: Real-time stock market data is sourced from IIFL Securities.

2. **Streaming**:
    - **Apache Kafka**: Used for building real-time data pipelines. Kafka captures the stock market tick data from IIFL Securities and streams it to Spark for processing.
    - **Apache ZooKeeper**: Manages and coordinates Kafka brokers.
    - **Apache Spark**: Distributed data processing engine that processes the real-time data. It consists of:
      - **Master Node**: Coordinates the distributed computation across worker nodes.
      - **Worker Nodes**: Execute tasks as part of the distributed processing.

3. **Deployment**:
    - **Docker**: Containers are used to deploy and manage the Kafka, Zookeeper, and Spark clusters.
    - **Amazon EC2**: Hosts the Docker containers and the entire streaming architecture in the cloud.

4. **Data Storage and Analytics**:
    - **AWS S3**: Processed data is stored in S3 buckets for persistence and further analysis.
    - **AWS Glue**: Manages ETL (Extract, Transform, Load) processes to clean, transform, and prepare data for analysis.
    - **AWS Athena**: Provides serverless interactive query services that analyze the data stored in S3 using standard SQL.
    - **Amazon Redshift**: A data warehouse service where the processed data can be further analyzed and visualized.

5. **Data Quality**:
    - **AWS Glue DataBrew**: Ensures data quality by detecting and handling anomalies, missing values, and other data issues before they are ingested into Redshift.

## Prerequisites

To run this project, ensure you have the following installed:

- Docker
- Apache Kafka
- Apache Spark
- AWS CLI (Configured with appropriate permissions)
- Python 3.x (For additional scripts and automation)

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/harshitgurani/stock_market_data-stream.git
   cd stock_market_data-stream





