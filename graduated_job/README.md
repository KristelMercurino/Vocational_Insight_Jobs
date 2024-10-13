
# Vocational Insight Jobs - Graduated Data Processing

## Overview

The **Vocational Insight Jobs - Graduated Data Processing** job is an automated pipeline designed to:

1. **Download** `.rar` files containing graduated job data from the [Ministerio de Educación de Chile](https://datosabiertos.mineduc.cl/titulados-en-educacion-superior/).
2. **Extract** the downloaded `.rar` files.
3. **Process** the contained CSV files to analyze and store data about graduates in a MySQL database.
4. **Log** all operations for monitoring and troubleshooting.
5. **Schedule** the entire process to run automatically every three months using `cron`.

## Features

- **Sequential Processing**: Downloads and processes one `.rar` file at a time to prevent duplication and ensure data integrity.
- **Database Integration**: Inserts processed data into a MySQL database, maintaining records of processed years to avoid reprocessing.
- **Logging**: Comprehensive logging of all operations, including downloads, extractions, processing steps, and errors.
- **Linux-Compatible**: Designed to run seamlessly in a Linux (Ubuntu) environment using compatible extraction tools like `unrar` or `7z`.
- **Automated Scheduling**: Utilizes `cron` to schedule the job to run every three months at 21:00 on the first day of the month.

## Prerequisites

Before setting up the job, ensure that your production environment meets the following requirements:

- **Operating System**: Ubuntu Linux
- **Python**: Version 3.8 or higher
- **MySQL Database**: Accessible with the necessary credentials
- **Extraction Tools**: `unrar` or `7z` installed on the system
- **Virtual Environment**: Recommended for managing Python dependencies

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/tu_usuario/Vocational_Insight_Jobs.git
   cd Vocational_Insight_Jobs/graduated_job
   ```

2. **Set Up the Virtual Environment**

   ```bash
   python3 -m venv env
   source env/bin/activate
   ```

3. **Install Python Dependencies**

   Ensure you have a `requirements.txt` file with the necessary packages:

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Install System Dependencies**

   Install `unrar` or `7z` for extracting `.rar` files:

   ```bash
   sudo apt-get update
   sudo apt-get install -y unrar p7zip-full
   ```

## Configuration

1. **Environment Variables**

   Create a `.env` file in the `graduated_job` directory with the following content:

   ```env
   # Database Configuration
   DB_USER=
   DB_PASS=""
   DB_HOST=
   DB_PORT=3306
   DB_NAME=

   # Extraction Tool Path (use either unrar or 7z)
   UNRAR_PATH="/usr/bin/unrar"  # For unrar
   # UNRAR_PATH="/usr/bin/7z"    # Uncomment if using 7z

   # Directories
   DOWNLOAD_DIR="/home/ubuntu/Vocational_Insight_Jobs/graduated_job/downloads"
   EXTRACT_DIR="/home/ubuntu/Vocational_Insight_Jobs/graduated_job/extracted"
   OUTPUT_CSV="/home/ubuntu/Vocational_Insight_Jobs/graduated_job/processed_data.csv"

   # Logging
   LOG_DIRECTORY="/home/ubuntu/Vocational_Insight_Jobs/graduated_job/logs"
   LOG_FILENAME="enrolled_job_logs.log"
   ```

   **Notes:**
   - Ensure all directories specified exist or the script has permissions to create them.
   - Secure the `.env` file to protect sensitive information.

2. **Database Setup**

   Ensure that the MySQL database specified in the `.env` file is accessible and that the user has the necessary permissions to create tables and insert data.

## Usage

To run the script manually, activate the virtual environment and execute the Python script:

```bash
source /home/ubuntu/Vocational_Insight_Jobs/graduated_job/env/bin/activate
python /home/ubuntu/Vocational_Insight_Jobs/graduated_job/main_linux.py --num-files 1
```

**Parameters:**

- `--num-files`: (Optional) Number of `.rar` files to download and process in one execution. Defaults to `1`.

## Scheduling with Cron

To automate the execution of the script every three months at 21:00 on the first day of the month, follow these steps:

1. **Edit the Crontab**

   Open the crontab editor:

   ```bash
   crontab -e
   ```

2. **Add the Cron Job Entry**

   Add the following line to schedule the job:

   ```cron
   0 21 1 */3 * source /home/ubuntu/Vocational_Insight_Jobs/graduated_job/env/bin/activate && python /home/ubuntu/Vocational_Insight_Jobs/graduated_job/main_linux.py --num-files 1 >> /home/ubuntu/Vocational_Insight_Jobs/graduated_job/logs/cron_job.log 2>&1
   ```

   **Explanation:**

   - `0 21 1 */3 *`: Runs at 21:00 on the 1st day of every 3rd month (January, April, July, October).
   - `source ... && python ...`: Activates the virtual environment and runs the Python script.
   - `>> ...cron_job.log 2>&1`: Appends both standard output and errors to `cron_job.log` for logging.

3. **Save and Exit**

   Save the crontab file and exit the editor. For `nano`, press `Ctrl + O` to save and `Ctrl + X` to exit.

4. **Verify the Cron Job**

   Ensure the cron job is added by listing all cron jobs:

   ```bash
   crontab -l
   ```

## Logging

All operations are logged both to a file and the console. Logs are stored in the directory specified by `LOG_DIRECTORY` in the `.env` file. For cron executions, additional logs are appended to `cron_job.log`.

**Log Files:**

- **General Logs:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/logs/enrolled_job_logs.log`
- **Cron Job Logs:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/logs/cron_job.log`

## Troubleshooting

- **Permissions Issues:** Ensure that the user running the cron job has read/write permissions to all specified directories.
- **Extraction Errors:** Verify that the `UNRAR_PATH` in the `.env` file points to the correct extraction tool and that it is installed.
- **Database Connection Problems:** Check that the database credentials are correct and that the database server is accessible from the production environment.
- **Missing Columns:** Ensure that the CSV files contain the expected columns, especially `area_carrera_generica_n`.

Check the log files for detailed error messages to assist in troubleshooting.

## Production Environment

The production environment is configured as follows:

- **Operating System:** Ubuntu Linux
- **Python Environment:** Virtual environment located at `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/env/`
- **Script Location:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/main_linux.py`
- **Directories:**
  - **Downloads:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/downloads`
  - **Extracted Files:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/extracted`
  - **Logs:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/logs`
  - **Processed CSV:** `/home/ubuntu/Vocational_Insight_Jobs/graduated_job/processed_data.csv`

Ensure that all paths in the `.env` file correctly reflect the production environment's directory structure.