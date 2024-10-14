# Scheduled Automation with Cron

This project includes an automated job scheduled to run **twice a year** using a **cron job**. Below are the details and setup instructions.

## Cron Job Details

- **Execution Dates:**  
  - **10th of April** at **22:00 (10:00 PM)**  
  - **10th of September** at **22:00 (10:00 PM)**  

- **Script Executed:**  
  `/home/ubuntu/Vocational_Insight_Jobs/enrolled_job/matriculados_automatizacion/matriculados_linux.py`

- **Virtual Environment Path:**  
  `/home/ubuntu/Vocational_Insight_Jobs/enrolled_job/env/bin/activate`

## Cron Job Command

This is the exact cron job configuration:

```bash
0 22 10 4,9 * /bin/bash -c "source /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/env/bin/activate && /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/matriculados_automatizacion/matriculados_linux.py"
```

## Setup Instructions

1. **Open Crontab Editor:**
   ```bash
   crontab -e
   ```

2. **Add the Cron Job:**
   Paste the following command inside the crontab editor:
   ```bash
   0 22 10 4,9 * /bin/bash -c "source /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/env/bin/activate && /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/matriculados_automatizacion/matriculados_linux.py"
   ```

3. **Save and Verify:**
   Save the changes and verify the cron job is scheduled:
   ```bash
   crontab -l
   ```

## Manual Execution (Optional)

To manually execute the script and verify that everything works:

```bash
source /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/env/bin/activate
python /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/matriculados_automatizacion/matriculados_linux.py
```

## Notes

- Ensure the script has the necessary permissions:
  ```bash
  chmod +x /home/ubuntu/Vocational_Insight_Jobs/enrolled_job/matriculados_automatizacion/matriculados_linux.py
  ```
- Make sure the virtual environment and Python dependencies are properly installed.