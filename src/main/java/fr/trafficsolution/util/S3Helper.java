package fr.trafficsolution.util;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * S3Helper - Utility class for S3 operations.
 */
public class S3Helper {
    private final S3Client s3Client;

    public S3Helper() {
        this.s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
    }

    public S3Helper(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Reads an object from S3 and returns its content as bytes.
     */
    public byte[] readObjectAsBytes(String bucketName, String key) {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return s3Client.getObjectAsBytes(getRequest).asByteArray();
    }

    /**
     * Reads an object from S3 and returns its content as a string.
     */
    public String readObjectAsString(String bucketName, String key) {
        byte[] bytes = readObjectAsBytes(bucketName, key);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads an object from S3 and returns an InputStream.
     */
    public InputStream readObjectAsStream(String bucketName, String key) {
        byte[] bytes = readObjectAsBytes(bucketName, key);
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Writes bytes to S3.
     */
    public void writeObject(String bucketName, String key, byte[] content) {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3Client.putObject(putRequest, RequestBody.fromBytes(content));
    }

    /**
     * Writes a string to S3.
     */
    public void writeObject(String bucketName, String key, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        writeObject(bucketName, key, bytes);
    }

    /**
     * Parses S3 location from message body.
     * Supports formats: "bucket/key", "s3://bucket/key", or just "key"
     */
    public S3Location parseLocation(String location) {
        if (location.startsWith("s3://")) {
            location = location.substring(5);
        }

        int firstSlash = location.indexOf('/');
        if (firstSlash > 0) {
            return new S3Location(location.substring(0, firstSlash), location.substring(firstSlash + 1));
        } else {
            return new S3Location(null, location);
        }
    }

    /**
     * Closes the S3 client.
     */
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    /**
     * Inner class to represent S3 location.
     */
    public static class S3Location {
        public final String bucket;
        public final String key;

        public S3Location(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }
}
