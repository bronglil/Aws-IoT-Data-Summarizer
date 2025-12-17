# IoT Traffic Processing – AWS Cloud Project

## 📌 Overview

This project implements a cloud-based system to ingest, summarize, consolidate, and export IoT traffic logs using AWS managed services. It is built with Java, leverages AWS Lambda and SQS for event-driven processing, and uses S3 as the central data store.

The architecture is designed to be:
- **Serverless** (using Lambda functions)
- **Reliable** (with SQS queues for decoupled processing)
- **Efficient** (by summarizing and consolidating traffic before exporting)

## 🧱 Architecture Summary

**Compute:**
- **EC2** – runs the `Upload Client` and `Export Client` (Java CLI apps)

**Storage:**
- **S3** – holds files in:
  - `incoming/`: raw CSVs
  - `summaries/`: daily summaries
  - `consolidated/`: cumulative stats
  - `exports/`: filtered output

**Messaging:**
- **SQS Queues**
  - `incoming-file-queue`: triggered by new uploads
  - `consolidator-queue`: triggered by summary completion

**Processing:**
- **AWS Lambda Functions**
  - `SummarizeWorker`: creates summarized CSVs
  - `ConsolidatorWorker`: maintains cumulative stats

## ⚙️ Prerequisites

- Java 17 (Amazon Corretto preferred)
- Maven
- AWS Learner Lab or AWS Free Tier account
- EC2 instance running Amazon Linux 2
- IAM Role: `LabRole` must be attached to your EC2 instance with permissions to:
  - `s3:*`
  - `sqs:*`
  - `lambda:InvokeFunction` (optional)

## 🧪 Local Environment Setup

### 1. Install Java 17 (Amazon Corretto)

**On Amazon EC2 (Amazon Linux 2):**
```bash
sudo dnf install java-17-amazon-corretto
sudo dnf install java-17-amazon-corretto-devel
```

### 2. Build JARs Using Maven

Clone the project or copy the codebase locally, then:

```bash
cd <project-root>
mvn clean package
```

This creates 3 executable JARs in the `target/` directory:
- `aws-cloud-1.0-SNAPSHOT-jar-with-dependencies.jar` (for Lambda)
- `upload-client-jar-with-dependencies.jar`
- `export-client-jar-with-dependencies.jar`

## ☁️ AWS Setup (One-Time)

### S3 Bucket Structure

Create an S3 bucket with the following structure (e.g. `my-iot-uploads-group12`):
```
my-iot-uploads-group12/
├── incoming/
├── summaries/
├── consolidated/
└── exports/
```

### SQS Queues

Create two **Standard** SQS queues:
- `incoming-file-queue`
- `consolidator-queue`

### Lambda Code Upload via S3 (RECOMMENDED)

1. Upload the Lambda JAR to your S3 bucket (outside of the folders above):
   ```
   aws-cloud-1.0-SNAPSHOT-jar-with-dependencies.jar
   ```

2. In Lambda Console:
   - Create two functions: `SummarizeWorker` and `ConsolidatorWorker`
   - Runtime: **Java 17**
   - Execution Role: **LabRole**
   - Under *Code*, choose “**Upload from Amazon S3**”
   - Paste the S3 URL of the JAR file (you can copy it from the file in the S3 Console)

3. Set the appropriate handler:
   - `SummarizeWorker`: `fr.trafficsolution.summarizer.SummarizeWorker::handleRequest`
   - `ConsolidatorWorker`: `fr.trafficsolution.consolidator.ConsolidateWorker::handleRequest`

4. Add **Environment Variables** to both functions:
   - `BUCKET_NAME = my-iot-uploads-group12`
   - For `SummarizeWorker`, also set:
     - `CONSOLIDATOR_QUEUE_URL = <Full URL of consolidator-queue>`

## 🧳 Uploading Files to EC2

### 1. Transfer JARs to EC2 via SFTP

Use FileZilla, WinSCP, or any SFTP tool.

- Protocol: `SFTP`
- Host: EC2 public IPv4
- Username: `ec2-user`
- Private Key: use the PEM key from your lab setup

Transfer these:
- `upload-client-jar-with-dependencies.jar`
- `export-client-jar-with-dependencies.jar`
- Sample CSV (`data.csv`)

## 🚀 Running the Project (Step-by-Step)

### Step 1: Upload Client (EC2)

On your EC2 instance:
```bash
export BUCKET_NAME=my-iot-uploads-group12
java -jar upload-client-jar-with-dependencies.jar /home/ec2-user/data.csv
```

🔍 Check S3 → `incoming/data.csv` should appear.

Once uploaded, S3 triggers the **SummarizeWorker**, which creates a summary file in `summaries/`, and then triggers **ConsolidatorWorker** to update `data_consolidated.csv` in `consolidated/`.

### Step 2: Verify Lambda Processing

Check these S3 folders:
- `summaries/` → contains summary CSVs per file
- `consolidated/` → has `data_consolidated.csv` continuously updated

Also, monitor CloudWatch logs for both Lambdas to confirm successful triggers.

### Step 3: Export Client (EC2)

Still on EC2:
```bash
export BUCKET_NAME=my-iot-uploads-group12
java -jar export-client-jar-with-dependencies.jar "192.168.1.10" "10.0.0.5"
```

This filters `data_consolidated.csv` for the given IP pair and creates a new file in:
```
S3 → exports/<timestamp>_filtered.csv
```

## ✅ Summary

You now have a full working AWS pipeline:
- Upload traffic logs
- Auto-trigger summarization and consolidation
- Filter and export as needed

It’s event-driven, decoupled, scalable, and cost-efficient. Perfect for analyzing distributed IoT traffic without maintaining your own servers.
