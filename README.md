IoT Traffic Processing – AWS Cloud Project
Overview

This project implements a cloud-based solution to upload, summarize, consolidate, and export IoT traffic data using AWS managed services.
The system is event-driven, serverless, and designed to be reliable, fast, and storage-efficient.

Architecture Summary

EC2: Upload Client and Export Client (Java applications)

S3: Stores CSV files (incoming/, summaries/, consolidated/, exports/)

SQS: Guarantees reliable processing

AWS Lambda: Summarize and Consolidate Workers

Prerequisites

Java 17

Maven

AWS Learner Lab (EC2, S3, SQS, Lambda)

IAM Role attached to EC2 (LabRole)

Build Instructions (Local Machine)

From the project root:

mvn clean package


This generates the following JAR files in target/:

aws-cloud-1.0-SNAPSHOT-jar-with-dependencies.jar (Lambda)

upload-client-jar-with-dependencies.jar

export-client-jar-with-dependencies.jar

AWS Setup (Once)
S3 Bucket Structure
my-iot-uploads-group12/
 ├── incoming/
 ├── summaries/
 ├── consolidated/
 └── exports/

SQS Queues

incoming-file-queue

consolidator-queue

Lambda Functions

SummarizeWorker

ConsolidatorWorker

Both Lambdas use:

Runtime: Java 17

Role: LabRole

Running the Project (Step by Step)
1. Upload Client (EC2)

Upload the JAR to EC2 using SFTP (FileZilla / WinSCP):

upload-client-jar-with-dependencies.jar


Run on EC2:

export BUCKET_NAME=my-iot-uploads-group12
java -jar upload-client-jar-with-dependencies.jar /home/ec2-user/data.csv


Result:

CSV uploaded to S3/incoming/

Summarize and Consolidate Lambdas triggered automatically

2. Verify Processing (AWS Console)

Check in S3:

summaries/ → summary CSV created

consolidated/ → data_consolidated.csv updated

3. Export Client (EC2)

Upload the Export Client JAR to EC2:

export-client-jar-with-dependencies.jar


Run on EC2 with a valid source/destination IP:

export BUCKET_NAME=my-iot-uploads-group12
java -jar export-client-jar-with-dependencies.jar "<SRC_IP>" "<DST_IP>"


Example:

java -jar export-client-jar-with-dependencies.jar "192.168.1.10" "10.0.0.5"


Result:

Filtered CSV created in S3/exports/
