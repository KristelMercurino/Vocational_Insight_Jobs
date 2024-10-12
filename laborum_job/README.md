Sure, a comprehensive setup is essential for deploying your web scraping bots effectively. Below, you'll find:

1. A **`requirements.txt`** file tailored for your bot scripts.
2. A **Markdown tutorial** that guides you through the entire process from development to production, including deployment on an EC2 instance, environment setup, execution, and scheduling with `cron`.

---

## 1. `requirements.txt`

This file lists all the Python dependencies your bot scripts require. Ensure that your project environment is clean and that you’re using compatible versions.

```plaintext
# requirements.txt

# Web Scraping
playwright==1.32.3

# Database ORM
SQLAlchemy==2.0.16
pymysql==1.0.3

# Environment Variables
python-dotenv==1.0.0

# Data Handling
pandas==2.0.3

# Logging
# (No additional packages needed; uses Python's built-in logging)

# Utilities
requests==2.31.0

# Optional: If using asynchronous Playwright
# playwright-asyncio==0.0.20
```

### **Notes:**

- **Playwright:** Ensure that Playwright browsers are installed after setting up the environment by running `playwright install`.
- **Versions:** Adjust the versions if you encounter compatibility issues, but the ones provided should work based on common usage.

---

## 2. Deployment Tutorial

### **Table of Contents**

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Step 1: Push Code to GitHub](#step-1-push-code-to-github)
4. [Step 2: Set Up EC2 Instance](#step-2-set-up-ec2-instance)
5. [Step 3: Pull Repository on EC2](#step-3-pull-repository-on-ec2)
6. [Step 4: Configure Environment Variables](#step-4-configure-environment-variables)
7. [Step 5: Create Virtual Environment](#step-5-create-virtual-environment)
8. [Step 6: Install Dependencies](#step-6-install-dependencies)
9. [Step 7: Execute the Scripts](#step-7-execute-the-scripts)
10. [Step 8: Set Up Crontab for Scheduling](#step-8-set-up-crontab-for-scheduling)
11. [Step 9: Verify Logs](#step-9-verify-logs)
12. [Conclusion](#conclusion)

---

### **Introduction**

This tutorial guides you through deploying your Python-based web scraping bots to an AWS EC2 instance. It covers pushing your code to GitHub, setting up the EC2 environment, configuring necessary variables, and scheduling the bots to run at specified intervals using `cron`.

---

### **Prerequisites**

- **AWS Account:** Ensure you have access to AWS and permissions to create and manage EC2 instances.
- **GitHub Account:** To host your repository.
- **SSH Key Pair:** For secure access to your EC2 instance.
- **Basic Knowledge:** Familiarity with Linux commands and Python environments.

---

### **Step 1: Push Code to GitHub**

1. **Initialize Git Repository:**
   
   ```bash
   git init
   ```

2. **Add Remote Repository:**
   
   ```bash
   git remote add origin https://github.com/yourusername/your-repo-name.git
   ```

3. **Add and Commit Code:**
   
   ```bash
   git add .
   git commit -m "Initial commit"
   ```

4. **Push to GitHub:**
   
   ```bash
   git push -u origin master
   ```

   > **Note:** Replace `master` with `main` if your repository uses `main` as the default branch.

---

### **Step 2: Set Up EC2 Instance**

1. **Launch an EC2 Instance:**
   
   - **AMI:** Choose Ubuntu Server (e.g., Ubuntu 22.04 LTS).
   - **Instance Type:** Depending on your scraping needs; `t2.micro` is sufficient for light tasks.
   - **Key Pair:** Select your existing key pair or create a new one.
   - **Security Group:** Ensure SSH (port 22) is open to your IP.

2. **Connect to EC2 via SSH:**
   
   ```bash
   ssh -i /path/to/your-key-pair.pem ubuntu@your-ec2-public-dns
   ```

---

### **Step 3: Pull Repository on EC2**

1. **Install Git (if not already installed):**
   
   ```bash
   sudo apt update
   sudo apt install git -y
   ```

2. **Clone Your Repository:**
   
   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   ```

3. **Navigate to the Project Directory:**
   
   ```bash
   cd your-repo-name
   ```

---

### **Step 4: Configure Environment Variables**

1. **Create a `.env` File:**
   
   ```bash
   nano .env
   ```

2. **Add Your Environment Variables:**
   
   ```env
   DB_USER=your_db_user
   DB_PASS=your_db_password
   DB_HOST=your_db_host
   DB_PORT=3306
   DB_NAME=your_db_name
   ```

3. **Save and Exit:**
   
   - Press `CTRL + X`, then `Y`, and `Enter` to save.

---

### **Step 5: Create Virtual Environment**

1. **Install Python and `venv` (if not already installed):**
   
   ```bash
   sudo apt install python3-pip python3-venv -y
   ```

2. **Create a Virtual Environment:**
   
   ```bash
   python3 -m venv env
   ```

3. **Activate the Virtual Environment:**
   
   ```bash
   source env/bin/activate
   ```

   > **Note:** Your terminal prompt should now start with `(env)`.

---

### **Step 6: Install Dependencies**

1. **Upgrade `pip`:**
   
   ```bash
   pip install --upgrade pip
   ```

2. **Install Required Packages:**
   
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Playwright Browsers:**
   
   ```bash
   playwright install
   ```

   > **Note:** This installs necessary browser binaries for Playwright.

---

### **Step 7: Execute the Scripts**

1. **Ensure Scripts are Executable:**
   
   ```bash
   chmod +x scraper_laborum.py scraper_laborum_subareas.py
   ```

2. **Run the Area Scraper:**
   
   ```bash
   python scraper_laborum.py
   ```

3. **Run the Subarea Scraper:**
   
   ```bash
   python scraper_laborum_subareas.py
   ```

   > **Note:** Ensure both scripts are working correctly before scheduling.

---

### **Step 8: Set Up Crontab for Scheduling**

1. **Open Crontab Editor:**
   
   ```bash
   crontab -e
   ```

2. **Add Cron Jobs:**
   
   ```cron
   # Run Area Scraper every 10 minutes
   */10 * * * * /home/ubuntu/your-repo-name/env/bin/python /home/ubuntu/your-repo-name/scraper_laborum.py >> /home/ubuntu/Vocational_Insight_Jobs/logs/area_scraper.log 2>&1

   # Run Subarea Scraper every 10 minutes
   */10 * * * * /home/ubuntu/your-repo-name/env/bin/python /home/ubuntu/your-repo-name/scraper_laborum_subareas.py >> /home/ubuntu/Vocational_Insight_Jobs/logs/subarea_scraper.log 2>&1

   # Run Area Scraper once a month (e.g., on the 1st at midnight)
   0 0 1 * * /home/ubuntu/your-repo-name/env/bin/python /home/ubuntu/your-repo-name/scraper_laborum.py >> /home/ubuntu/Vocational_Insight_Jobs/logs/area_scraper_monthly.log 2>&1

   # Run Subarea Scraper once a month (e.g., on the 1st at 1 AM)
   0 1 1 * * /home/ubuntu/your-repo-name/env/bin/python /home/ubuntu/your-repo-name/scraper_laborum_subareas.py >> /home/ubuntu/Vocational_Insight_Jobs/logs/subarea_scraper_monthly.log 2>&1
   ```

   > **Notes:**
   >
   > - **Path Adjustments:** Replace `/home/ubuntu/your-repo-name/` with the actual path to your scripts.
   > - **Logging:** Logs are directed to specific log files for monitoring.
   > - **Cron Syntax:** `*/10 * * * *` runs the task every 10 minutes. Adjust the monthly cron jobs as needed.

3. **Save and Exit:**
   
   - In `nano`, press `CTRL + X`, then `Y`, and `Enter`.

4. **Verify Crontab Entries:**
   
   ```bash
   crontab -l
   ```

---

### **Step 9: Verify Logs**

Ensure that logs are being created and updated correctly.

1. **Navigate to the Logs Directory:**
   
   ```bash
   cd /home/ubuntu/Vocational_Insight_Jobs/logs
   ```

2. **View Logs:**
   
   ```bash
   tail -f enrolled_job_logs.log
   tail -f area_scraper.log
   tail -f subarea_scraper.log
   ```

   > **Note:** Use `CTRL + C` to stop `tail -f`.

3. **Check for Errors:**
   
   - Ensure no errors are present in the logs.
   - Confirm that data is being scraped and inserted into the database as expected.

---

### **Conclusion**

By following the above steps, you have successfully:

- **Prepared your Python environment** with all necessary dependencies.
- **Deployed your scraping scripts** to an AWS EC2 instance.
- **Configured environment variables** securely.
- **Automated the execution** of your scripts using `cron` for periodic data scraping.
- **Implemented logging** to monitor the execution and troubleshoot any issues.

### **Additional Recommendations**

- **Security:**
  
  - Ensure your `.env` file is excluded from version control by adding it to `.gitignore`.
  - Regularly update your dependencies to patch any security vulnerabilities.

- **Monitoring:**
  
  - Implement alerting mechanisms (e.g., email notifications) for critical failures or anomalies in your scraping process.
