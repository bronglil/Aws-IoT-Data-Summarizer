package fr.trafficsolution.client;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * UploadClient - Optional utility for testing locally.
 * 
 * Purpose: Simulates branch uploading by uploading raw CSV files to S3
 * under prefix incoming/ and optionally sending SQS messages.
 * 
 * This is NOT needed for AWS Lambda or EC2 deployment, but useful for testing.
 */
public class UploadClient {
    private final S3Client s3Client;
    private final SqsClient sqsClient;
    private final String bucketName;
    private final String summarizeQueueUrl;

    public UploadClient(String bucketName, String summarizeQueueUrl) {
        this.s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        this.sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();
        this.bucketName = bucketName;
        this.summarizeQueueUrl = summarizeQueueUrl;
    }

    /**
     * Uploads a CSV file to S3 under incoming/ prefix.
     * 
     * @param localFilePath Path to the local CSV file
     * @param s3FileName    Name to use in S3 (will be placed under incoming/)
     * @throws IOException If file cannot be read or uploaded
     */
    public void uploadCsvToS3(String localFilePath, String s3FileName) throws IOException {
        Path filePath = Paths.get(localFilePath);

        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + localFilePath);
        }

        byte[] fileContent = Files.readAllBytes(filePath);
        String s3Key = "incoming/" + s3FileName;

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3Client.putObject(putRequest, RequestBody.fromBytes(fileContent));

        System.out.println("Uploaded: " + localFilePath + " -> s3://" + bucketName + "/" + s3Key);

        // Optionally send SQS message to trigger summarization
        if (summarizeQueueUrl != null && !summarizeQueueUrl.isEmpty()) {
            sendSqsMessage(s3Key);
        }
    }

    /**
     * Uploads a CSV file to S3 and sends SQS message.
     * 
     * @param localFilePath Path to the local CSV file
     * @throws IOException If file cannot be read or uploaded
     */
    public void uploadCsvToS3(String localFilePath) throws IOException {
        Path filePath = Paths.get(localFilePath);
        String fileName = filePath.getFileName().toString();
        uploadCsvToS3(localFilePath, fileName);
    }

    /**
     * Sends an SQS message with the S3 file location.
     */
    private void sendSqsMessage(String s3Key) {
        if (summarizeQueueUrl == null || summarizeQueueUrl.isEmpty()) {
            return;
        }

        String messageBody = bucketName + "/" + s3Key;

        SendMessageRequest sendRequest = SendMessageRequest.builder()
                .queueUrl(summarizeQueueUrl)
                .messageBody(messageBody)
                .build();

        sqsClient.sendMessage(sendRequest);
        System.out.println("Sent SQS message: " + messageBody);
    }

    /**
     * Closes all clients.
     */
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (sqsClient != null) {
            sqsClient.close();
        }
    }

    /**
     * Example usage for testing.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: UploadClient <bucket-name> <local-csv-file> [summarize-queue-url]");
            System.out.println(
                    "Example: UploadClient my-bucket data.csv https://sqs.us-east-1.amazonaws.com/123456789/summarize-queue");
            System.exit(1);
        }

        String bucketName = args[0];
        String localFile = args[1];
        String queueUrl = args.length > 2 ? args[2] : null;

        UploadClient client = new UploadClient(bucketName, queueUrl);

        try {
            client.uploadCsvToS3(localFile);
            System.out.println("Upload completed successfully!");
        } catch (IOException e) {
            System.err.println("Error uploading file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
