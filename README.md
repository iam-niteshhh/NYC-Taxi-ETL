# NYC ETC DATA PIPELINE 🚀

## Project Overview

This project automates the **Extract**, **Transform**, and **Load (ETL)** pipeline using **Apache Airflow**. The pipeline is designed to fetch raw data, clean and transform it, and load it into a PostgreSQL database for further analysis. The pipeline is fully automated, production-ready, and orchestrated using **Apache Airflow**.

### Key Features:
- **Automated Workflow**: The pipeline fetches raw data, cleans it, and loads it into a PostgreSQL database.
- **Data Storage**: Uses **Amazon S3** for raw and transformed data storage, with **PostgreSQL** for final data warehousing.
- **Scalable & Production-Ready**: The system is easily deployable and scalable with **Docker** and **Docker Compose**.
- **Monitoring & Scheduling**: Managed using **Apache Airflow**, ensuring task dependencies and schedules are efficiently handled.

---

## Technologies Used 💻

- **Orchestration**:
  - **Apache Airflow** (2.7)
  - **Docker & Docker Compose**

- **Programming Language**:
  - **Python** (with **Pandas** for data transformations)

- **Data Storage**:
  - **Amazon S3** (for staging raw and transformed data)
  - **PostgreSQL** (final structured data warehouse)

- **Cloud Integration**:
  - **Boto3** (for AWS S3 interactions)

---
