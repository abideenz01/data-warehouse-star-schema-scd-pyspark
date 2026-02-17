# End-to-End Data Warehouse with PySpark & Medallion Architecture

## 🚀 Project Overview
This project demonstrates a production-grade data pipeline using **PySpark** and the **Medallion Architecture**. It handles the full lifecycle of data from raw ingestion to a structured Star Schema, implementing advanced Data Warehousing techniques like **Slowly Changing Dimensions (SCD)**.

## 🏗️ Architecture
- **Bronze (Ingestion):** Raw data ingestion from source systems.
- **Silver (Staging):** Data cleaning, schema enforcement, and implementation of **SCD Type 1 & Type 2** logic.
- **Gold (Analytics):** Production-ready **Fact and Dimension tables** optimized for BI reporting.

## 🛠️ Tech Stack
- **Language:** Python (PySpark)
- **Storage:** Delta Lake (for ACID transactions)
- **Orchestration:** Modular Python scripts
- **Environment:** Databricks / Local Spark

## 📂 Repository Structure
- `src/bronze/`: Ingestion logic.
- `src/silver/`: SCD Type 1/2 logic and cleaning.
- `src/gold/`: Fact and Dimension table modeling.
- `src/utils/`: Data quality and validation checks.
- `notebooks/`: Exploratory Data Analysis (EDA) and prototyping.

## 📈 Key Features
- **SCD Type 2:** Tracks historical changes for key dimensions (e.g., Product/Customer).
- **Data Validation:** Automated checks for nulls and schema drifts before loading Gold tables.
- **Modular Design:** Code is organized into reusable modules rather than monolithic notebooks.

---


