# ETL Pipeline with Python and Workflow Orchestration with Airflow

This project is a demonstration of an end-to-end data pipeline built using modern data engineering tools. It features an ETL script for processing data from Excel files into a PostgreSQL database, and an Airflow DAG (`excel_etl_dag.py`) that orchestrates and schedules this entire process automatically.

## Project Overview and Features

This project was developed to showcase the following key competencies in data engineering:

*   **ETL Process Development:** The `excel_to_postgres_etl.py` script reads data from local Excel files, processes it using **Pandas**, and loads it into a **PostgreSQL** database leveraging **SQLAlchemy**.
*   **Workflow Orchestration:** The `excel_etl_dag.py` defines a simple but robust Directed Acyclic Graph (DAG) in **Apache Airflow** to schedule and run the ETL script on a daily basis.
*   **Professional Practices:** The project is structured with best practices, including dynamic file paths for portability, professional logging instead of print statements, and dependency management with `requirements.txt`.

## Tech Stack

*   **Language:** Python
*   **Orchestrator:** Apache Airflow
*   **Database:** PostgreSQL
*   **Core Libraries:** Pandas, SQLAlchemy

## Getting Started

### Prerequisites

*   Python 3.x
*   An running instance of Apache Airflow
*   A running instance of PostgreSQL

### Installation

1.  Clone this repository:
    ```bash
    git clone <your-repository-url>
    ```
2.  Install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Important:** For the database connection to work, the following environment variables must be set in your system:
    *   `PGPASS`: Your PostgreSQL password
    *   `PGUID`: Your PostgreSQL user
 
4.  Place the `dags` folder content into your Airflow `dags` folder and ensure the `scripts` folder is accessible by Airflow.

## About the Maintainer

This project is currently maintained by Priyal Shah, a Software Developer with 4+ years of professional experience in data engineering and analytics. Priyal specializes in SQL, Python (Pandas, NumPy), R, and DAX.

*   **Email:** priyalshah04845@gmail.com
*   **GitHub:** [Your GitHub Profile]
*   **LinkedIn:** [Your LinkedIn Profile]