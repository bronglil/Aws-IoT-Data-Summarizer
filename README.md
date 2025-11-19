# AWS Cloud Traffic Solution

A Java project with two worker modules for processing and consolidating traffic data from S3.

## Project Structure

```
aws-cloud/
├── pom.xml                          # Maven configuration
├── src/
│   └── main/
│       └── java/
│           └── fr/
│               └── traffic-solution/
│                   ├── SummarizeWorker.java      # Module 1: Reads CSV, computes totals
│                   ├── ConsolidatorWorker.java  # Module 2: Consolidates summaries
│                   └── ExampleUsage.java        # Usage examples
└── README.md
```

## Modules

### 1. SummarizeWorker

**Purpose**: Reads a CSV file from S3, computes totals per (Src, Dst, date) combination, and writes a summary file back to S3.

**Features**:
- Reads CSV files from S3
- Parses and aggregates data by Source, Destination, and Date
- Writes summary CSV files back to S3
- Handles header rows automatically
- Supports flexible CSV formats

**Usage**:
```java
SummarizeWorker worker = new SummarizeWorker();
worker.process("bucket-name", "input/data.csv", "summaries/summary.csv");
worker.close();
```

**Expected Input CSV Format**:
```
Src, Dst, Date, Value
A, B, 2024-01-01, 100.5
A, B, 2024-01-01, 50.0
...
```

**Output Summary Format**:
```
Src, Dst, Date, Total
A, B, 2024-01-01, 150.5
...
```

### 2. ConsolidatorWorker

**Purpose**: Reads multiple summary files from S3, consolidates them, and updates aggregate totals.

**Features**:
- Reads multiple summary files from S3 (by prefix or specific keys)
- Merges summaries and aggregates totals
- Writes consolidated results back to S3
- Handles pagination for large S3 listings

**Usage**:
```java
// Consolidate all summaries with a prefix
ConsolidatorWorker worker = new ConsolidatorWorker();
worker.consolidateSummaries("bucket-name", "summaries/", "aggregates/consolidated.csv");
worker.close();

// Or consolidate specific files
List<String> files = Arrays.asList("summaries/file1.csv", "summaries/file2.csv");
worker.consolidateSummaries("bucket-name", files, "aggregates/consolidated.csv");
```

## Dependencies

- **AWS SDK for Java 2.x**: For S3 operations
- **OpenCSV**: For CSV parsing and writing
- **Java 17**: Required Java version

## Building the Project

```bash
# Compile the project
mvn compile

# Package the project (creates JAR with dependencies)
mvn package

# Run tests (if any)
mvn test
```

## Configuration

### AWS Credentials

The workers use the default AWS credential chain. Make sure you have:
- AWS credentials configured via `~/.aws/credentials`, or
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), or
- IAM role (if running on EC2/Lambda)

### Region Configuration

By default, both workers use `US_EAST_1` region. To change:

```java
S3Client s3Client = S3Client.builder()
    .region(Region.US_WEST_2)  // Your preferred region
    .build();
    
SummarizeWorker worker = new SummarizeWorker(s3Client);
```

## Example Workflow

1. **Process raw data**:
   ```java
   SummarizeWorker summarizeWorker = new SummarizeWorker();
   summarizeWorker.process("my-bucket", "raw-data/input.csv", "summaries/summary1.csv");
   summarizeWorker.close();
   ```

2. **Consolidate multiple summaries**:
   ```java
   ConsolidatorWorker consolidatorWorker = new ConsolidatorWorker();
   consolidatorWorker.consolidateSummaries("my-bucket", "summaries/", "aggregates/final.csv");
   consolidatorWorker.close();
   ```

## Notes

- Date format: ISO_LOCAL_DATE (YYYY-MM-DD)
- The workers automatically handle CSV headers
- Summary files are sorted by (Src, Dst, Date) in the output
- All S3 operations use UTF-8 encoding

