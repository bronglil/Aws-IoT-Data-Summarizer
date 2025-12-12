#IoT Traffic Processing – AWS Cloud Project
##Overview

This project implements a cloud-based solution to upload, summarize, consolidate, and export IoT traffic data using AWS managed services. The system is event-driven, serverless, and designed to be reliable, fast, and storage-efficient.
Architecture Summary

EC2 – Upload Client and Export Client (Java applications)
S3 – Stores CSV files (incoming/, summaries/, consolidated/, exports/)
SQS – Guarantees reliable message processing
AWS Lambda – Summarize and Consolidate Workers

Prerequisites
```
Java 17
Maven
AWS Learner Lab access
AWS services:

EC2
S3
SQS
Lambda


IAM Role attached to EC2: LabRole

Build Instructions (Local Machine)
From the project root directory, run:

mvn clean package
```
## Code Structure
```

This command generates the following JAR files in the `target/` directory:

- `aws-cloud-1.0-SNAPSHOT-jar-with-dependencies.jar` (Lambda)
- `upload-client-jar-with-dependencies.jar`
- `export-client-jar-with-dependencies.jar`

## AWS Setup (One-Time)

### S3 Bucket Structure
my-iot-uploads-group12/
 ├── incoming/
 ├── summaries/
 ├── consolidated/
 └── exports/


### SQS Queues

- `incoming-file-queue`
- `consolidator-queue`

### Lambda Functions

- `SummarizeWorker`
- `ConsolidatorWorker`

Both Lambda functions use:

- Runtime: Java 17
- Execution Role: LabRole

## Running the Project (Step by Step)

### 1. Upload Client (EC2)

Upload the following JAR file to EC2 using SFTP (FileZilla or WinSCP):
```
upload-client-jar-with-dependencies.jar

## Run the Upload Client on EC2:

export BUCKET_NAME=my-iot-uploads-group12
java -jar upload-client-jar-with-dependencies.jar /home/ec2-user/data.csv
```

**Result:**

- CSV file uploaded to `S3/incoming/`
- Summarize and Consolidate Lambda functions are triggered automatically

### 2. Verify Processing (AWS Console)

Check the S3 bucket:

- `summaries/` → summarized CSV file created
- `consolidated/` → `data_consolidated.csv` updated

### 3. Export Client (EC2)

Upload the Export Client JAR to EC2:
```
export-client-jar-with-dependencies.jar


## Run the Export Client with a valid source and destination IP:

```
export BUCKET_NAME=my-iot-uploads-group12
java -jar export-client-jar-with-dependencies.jar "<SRC_IP>" "<DST_IP>"
```
