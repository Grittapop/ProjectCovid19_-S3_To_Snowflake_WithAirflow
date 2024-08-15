# ProjectCovidTH_S3_To_Snowflake_WithAirflow
This repository contains an Airflow DAG for fetching, transforming, and loading COVID-19 patient information from the Thailand Ministry of Public Health API into Snowflake. The pipeline also includes Slack notifications upon successful data load.

## Architecture

![s3_to_Snowflake](https://github.com/user-attachments/assets/a555f624-3473-4386-8e26-2e9c075cde02)

## Technologies
- Python
- Docker
- Apache Airflow
- Snowflake
- Slack
## Starting Airflow
Use Docker Compose to start Airflow and its dependencies:
```yaml
docker-compose up -d --build
```

To stop the Docker containers, use:
```yaml
docker-compose down
```
## Running the Pipeline
Once Docker Compose is running, you can access the Airflow web interface at **http://localhost:8080**.

Log in with the default credentials:

**Username**: airflow

**Password**: airflow

## Create a Slack Webhook
#### 1. Log in to Slack:
- Log in to your Slack account via [Slack Workspace](https://slack.com).

#### 2. Navigate to Apps:
- In your Slack workspace, click on **"Apps"** and search for **"Incoming Webhooks."**

  ![Screenshot 2024-08-15 170045](https://github.com/user-attachments/assets/9fc15a40-5747-4343-bac5-91957df58a93)


#### 3. Enable Incoming Webhooks:

- Click on "Add to Slack" to enable Incoming Webhooks.
- Choose the channel where you want the Webhook to send messages, then click **"Allow."**

  ![Screenshot 2024-08-15 170144](https://github.com/user-attachments/assets/6e2636b9-43cb-4964-9179-f4318ee72db5)


#### 4.Create a Webhook URL:
- After selecting the channel, Slack will generate a Webhook URL for you.
- Copy this URL to use it in your application (in Airflow).

  ![Screenshot 2024-08-15 170325](https://github.com/user-attachments/assets/3a179c45-2b34-4356-a42e-2e996ab75e98)


#### 5.Additional Configuration (if needed):
- You can customize the default message, bot name, and profile picture for the Webhook.
- After configuring, click "Save Settings."

#### 6.Use the Webhook URL:
- In your application (e.g., Airflow), use the Webhook URL generated in step 4.
- For Airflow, you can set the Slack Webhook URL using as Password in **Airflow Connection**.

![Screenshot 2024-03-06 023651](https://github.com/user-attachments/assets/b696377a-5fe4-47d8-bf80-14b2abb55dc7)

## Create an S3 Bucket
1. Click on the Create bucket button.
2. Enter a Bucket name **bucket-weekly-covid-patient-information**.
3. Select an AWS Region **ap-southeast-1**.
4. Configure other options as needed (e.g., Bucket versioning, encryption).
5. Click Create bucket at the bottom of the page.

Now, your S3 bucket is created and ready for use.

Make sure to create an **IAM** Role with full permissions to access S3 to get the **AWS Access Key** and **AWS Secret Access Key**.

![Screenshot 2024-03-06 022603](https://github.com/user-attachments/assets/cd7e91eb-21ff-48a5-96ac-95bc13a0208b)






