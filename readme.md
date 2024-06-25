# Airflow Project

This repository contains DAGs for Apache Airflow to manage JOB parser data pipeline.

## Table of Contents

1. [Installation](#installation)
2. [Setup](#setup)
3. [Creating a User](#creating-a-user)
4. [Usage](#usage)
5. [Screenshots](#screenshots)
6. [Resources](#resources)

## Installation

### Prerequisites

- Python 3.10 or higher
- Pip
- Virtualenv (recommended)
- [tg_jobs_parser module](https://github.com/WuDMC/tg_jobs_parser_module)

### Steps

1. **Clone the repository**:

    ```sh
    cd your-airflow-project
    git clone https://github.com/WuDMC/ELT_DAGs_for_tg_jobs_parser.git
    ```


2.  **Change path to tg_jobs_parser module at makefile**:

    ```sh
    nano makefile
    pip install -e /path/to/jobs_parser/ 
    ```
3.  **Install dependencies**:

    ```sh
    make install
    ```

4. **Set the `AIRFLOW_HOME` environment variable to PyCharm (for debugging)**:

    ```sh
    AIRFLOW_HOME=/path/to/airflow
    ```
   [Video Tutorial](https://www.youtube.com/watch?v=lYhag-yNtIQ)
   
## Setup Airflow (if not done before)

1. **Initialize the Airflow database **:

    ```sh
    airflow db init
    ```

2. **Create an admin user**:

    ```sh
    airflow users create --role Admin --username User --email admin --firstname admin --lastname admin --password Pass
    ```

## Usage

1. **Start the Airflow Standalone or test from PyCharm**:

    ```sh
    make run (from project dir)
    ```


3. **Access the Airflow UI** by navigating to `http://localhost:8080` in your web browser.

## Screenshots

### Airflow UI

![Airflow UI](docs/images/airflow-ui.png)

### DAGs Dashboard

![DAGs Dashboard](docs/images/dags-dashboard.png)

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Video Tutorial](https://www.youtube.com/watch?v=lYhag-yNtIQ)
