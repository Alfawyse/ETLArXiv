# ArXiv ETL Pipeline

## Project Overview

This project automates the extraction, transformation, and loading (ETL) process for research articles from ArXiv. The articles are parsed from XML format, processed, and then uploaded to an Amazon S3 bucket. It also integrates with a PostgreSQL database to store article metadata.

Additionally, a separate script is used to scrape category taxonomy data from the ArXiv website, which is not available via the ArXiv API.

## Features

- **ETL Workflow with Apache Airflow:** Automated ETL process orchestrated by Airflow.
- **XML Parsing:** Extracts authors, affiliations, categories, and links from ArXiv article XML files.
- **Data Transformation:** Transforms extracted XML data into a structured format (Pandas DataFrame) and saves it as a CSV file.
- **Database Integration:** Inserts parsed data into a PostgreSQL database.
- **S3 Upload:** Uploads the generated CSV and other data files to an S3 bucket, with the option to delete local files after successful uploads.
- **Web Scraping:** Scrapes the category taxonomy from ArXiv as it is not available through the official API.

## Project Structure

```
ETLArXiv/
├── config/
│   └── settings.py           # Configuration file for database credentials
├── dags/
│   └── arxiv_etl_dag.py      # DAG definition for the ETL process with Airflow
├── data/                     # Directory where XML and CSV files are stored
├── logs/                     # Directory for logs
├── scripts/
│   ├── extract_data.py        # Script to extract data from XML files
│   ├── load_data.py           # Script to load data into the PostgreSQL database
│   ├── scrap_taxonomy_data.py # Script to scrape category taxonomy from ArXiv
│   ├── transform_data.py      # Script to transform XML data to CSV
│   ├── upload_to_s3.py        # Script to upload data to an S3 bucket
│   └── utils.py               # Utility functions used across scripts
├── venv/                      # Virtual environment (not included in the repository)
├── .env                       # Environment variables for database and S3 configurations
├── .gitignore                 # Files to ignore in version control
├── README.md                  # Documentation for the project
└── requirements.txt           # Python dependencies
```

## Detailed Explanation of Each File

### 1. `config/settings.py`
This file contains the configuration needed to connect to the PostgreSQL database. It loads the database credentials from the `.env` file.

### 2. `dags/arxiv_etl_dag.py`
Defines the DAG (Directed Acyclic Graph) for Apache Airflow, which orchestrates the ETL process. It defines the following tasks and their execution order:
- **`extract_task`**: Calls `extract_data.py` to fetch XML data from ArXiv.
- **`transform_task`**: Calls `transform_data.py` to parse and transform the XML data into a structured format (CSV).
- **`load_task`**: Calls `load_data.py` to load the processed data into the PostgreSQL database.
- **`upload_task`**: Calls `upload_to_s3.py` to upload the processed files to an S3 bucket.

The execution order follows a linear flow:
```
extract_task >> transform_task >> load_task >> upload_task
```

### 3. `scripts/extract_data.py`
This script handles the extraction of data from ArXiv's XML files. It parses the article data and extracts metadata such as authors, affiliations, categories, and links. The extracted data is stored in a temporary location for further transformation.

### 4. `scripts/transform_data.py`
Transforms the XML data extracted by `extract_data.py` into a structured format using Pandas. It creates a DataFrame with relevant fields such as authors, affiliations, categories, etc. The final data is saved as a CSV file in the `data/` directory.

### 5. `scripts/load_data.py`
This script takes the transformed CSV data and inserts it into the PostgreSQL database. It ensures that each article and its associated metadata (authors, categories, etc.) are stored in their corresponding database tables.

### 6. `scripts/upload_to_s3.py`
Uploads the generated CSV files and other data to an Amazon S3 bucket. After successful uploads, it deletes the local files to free up storage space.

### 7. `scripts/scrap_taxonomy_data.py`
A separate script used to scrape the category taxonomy from the ArXiv website. This data is not available via the ArXiv API, so the script extracts it directly from the HTML structure of the website. The scraped taxonomy includes main and subcategories, which are saved for further use in the ETL process or database insertion.

### 8. `.env`
This file contains environment variables needed for the project. It stores sensitive information such as:
- PostgreSQL credentials
- S3 bucket name

The structure of the `.env` file is as follows:

```
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=your_db_port
BUCKET_NAME=your-s3-bucket-name
```

## ETL Pipeline Overview

The ETL process follows a structured order of execution using Apache Airflow:

1. **Extract**: The `extract_data.py` script extracts article data from ArXiv in XML format.
2. **Transform**: The `transform_data.py` script processes and transforms the XML data into a structured CSV format.
3. **Load**: The `load_data.py` script inserts the transformed data into the PostgreSQL database.
4. **Upload**: Finally, the `upload_to_s3.py` script uploads the processed CSV files to an S3 bucket.

These tasks are managed by the DAG defined in `arxiv_etl_dag.py` and executed in the following order:

```
extract_task >> transform_task >> load_task >> upload_task
```
# How to Use

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-repo-name.git
   cd your-repo-name
   ```

2. **Set up the environment:**

   Ensure your `.env` file is configured with your database and S3 bucket credentials.

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Data Extraction:**

   To extract the data from the XML files in the `data` folder:

   ```bash
   python scripts/extract_data.py

5. **Run the Data Transformation:**

   To process the XML files in the `data` folder and transform them into a CSV file:

   ```bash
   python scripts/transform_data.py
   ```
   

6. **Load Data to PostgreSQL:**

   To load the transformed CSV data into the PostgreSQL database:

   ```bash
   python scripts/load_data.py
   ```

7. **Upload Files to S3:**

   After processing the data, you can upload the CSV and other files in the `data` folder to S3 by running:

   ```bash
   python scripts/upload_to_s3.py
   ```



## Airflow ETL Pipeline

The ETL process is also orchestrated using Airflow. The DAG for this process is located in `dags/arxiv_etl_dag.py`.

To run Airflow:

1. **Initialize Airflow database:**

   ```bash
   airflow db init
   ```

2. **Start the scheduler and web server:**

   ```bash
   airflow scheduler
   airflow webserver
   ```

3. **Access the Airflow UI:**

   Open your browser and navigate to `http://localhost:8080` to monitor your DAG and trigger the ETL process.
