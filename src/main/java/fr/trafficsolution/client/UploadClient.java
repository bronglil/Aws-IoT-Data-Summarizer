package fr.trafficsolution.client;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.nio.file.Paths;

public class UploadClient {

    public static void main(String[] args) {
        // Simple argument handling:
        // args[0] = local file path
        // args[1] = bucket name (optional, fallback to env BUCKET_NAME)
        // args[2] = S3 key (optional, auto-derive if missing)

        if (args.length < 1) {
            System.out.println("Usage: java -jar upload-client.jar <localFilePath> [bucketName] [s3Key]");
            System.exit(1);
        }

        String localFilePath = args[0];

        String bucket = (args.length >= 2 && !args[1].isEmpty())
                ? args[1]
                : System.getenv("BUCKET_NAME");  // reuse same env var name

        if (bucket == null || bucket.isEmpty()) {
            System.err.println("Bucket name not provided and BUCKET_NAME env var is not set.");
            System.exit(1);
        }

        // If no explicit key, put it under incoming/<filename>
        String s3Key;
        if (args.length >= 3 && !args[2].isEmpty()) {
            s3Key = args[2];
        } else {
            String fileName = Paths.get(localFilePath).getFileName().toString();
            s3Key = "incoming/" + fileName;
        }

        System.out.println("Uploading file:");
        System.out.println("  Local:  " + localFilePath);
        System.out.println("  Bucket: " + bucket);
        System.out.println("  Key:    " + s3Key);

        File file = new File(localFilePath);
        if (!file.exists() || !file.isFile()) {
            System.err.println("Local file does not exist: " + localFilePath);
            System.exit(1);
        }

        // Create S3 client (region us-east-1, credentials from instance role in Learner Lab)
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3Key)
                .contentType("text/csv")
                .build();

        s3.putObject(putReq, RequestBody.fromFile(file.toPath()));

        System.out.println("Upload complete: s3://" + bucket + "/" + s3Key);
    }
}
