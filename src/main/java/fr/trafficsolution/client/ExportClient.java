package fr.trafficsolution.client;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

public class ExportClient {

    public static void main(String[] args) {

        // Usage:
        // java -jar export-client-jar-with-dependencies.jar <src_ip> <dst_ip> [bucketName]
        if (args.length < 2) {
            System.out.println("Usage: java -jar export-client-jar-with-dependencies.jar <src_ip> <dst_ip> [bucketName]");
            System.exit(1);
        }

        String srcIp = args[0].trim();
        String dstIp = args[1].trim();

        String bucket = (args.length >= 3 && !args[2].isEmpty())
                ? args[2].trim()
                : System.getenv("BUCKET_NAME");

        if (bucket == null || bucket.isEmpty()) {
            System.err.println("Bucket name not provided and BUCKET_NAME env var is not set.");
            System.exit(1);
        }

        String consolidatedKey = "consolidated/data_consolidated.csv";

        // Output file key
        String safeSrc = srcIp.replaceAll("[^0-9A-Za-z.:-]", "_");
        String safeDst = dstIp.replaceAll("[^0-9A-Za-z.:-]", "_");
        String outputKey = "exports/" + safeSrc + "__" + safeDst + ".csv";

        System.out.println("Export request:");
        System.out.println("  Bucket: " + bucket);
        System.out.println("  Read:   s3://" + bucket + "/" + consolidatedKey);
        System.out.println("  Filter: src_ip=" + srcIp + "  dst_ip=" + dstIp);
        System.out.println("  Write:  s3://" + bucket + "/" + outputKey);

        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        // 1) Download consolidated CSV
        ResponseBytes<GetObjectResponse> bytes = s3.getObjectAsBytes(
                GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(consolidatedKey)
                        .build()
        );

        String content = bytes.asString(StandardCharsets.UTF_8);

        // 2) Filter lines
        String[] lines = content.split("\\r?\\n");
        if (lines.length == 0) {
            System.err.println("Consolidated file is empty.");
            System.exit(1);
        }

        String header = lines[0].trim();
        StringBuilder out = new StringBuilder();
        out.append(header).append("\n");

        int matches = 0;

        // Expected consolidated columns (example):
        // date,src_ip,dst_ip,avg_flow_duration,stddev_flow_duration,avg_fwd_pkts,stddev_fwd_pkts
        // We'll locate src/dst columns by header name if possible, fallback to indexes 1 and 2
        int srcIdx = 1;
        int dstIdx = 2;

        String[] headCols = header.split(",", -1);
        for (int i = 0; i < headCols.length; i++) {
            String c = headCols[i].trim().toLowerCase();
            if (c.equals("src_ip") || c.equals("src ip")) srcIdx = i;
            if (c.equals("dst_ip") || c.equals("dst ip")) dstIdx = i;
        }

        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            if (line == null) continue;
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] cols = line.split(",", -1);
            if (cols.length <= Math.max(srcIdx, dstIdx)) continue;

            String s = cols[srcIdx].trim();
            String d = cols[dstIdx].trim();

            if (srcIp.equals(s) && dstIp.equals(d)) {
                out.append(line).append("\n");
                matches++;
            }
        }

        System.out.println("Matched rows: " + matches);

        // If no matches, still write file with header (useful for debugging)
        if (matches == 0) {
            out.append("# No matching rows for src_ip=").append(srcIp)
               .append(" dst_ip=").append(dstIp)
               .append(" at ").append(LocalDateTime.now())
               .append("\n");
        }

        // 3) Upload result to exports/
        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(bucket)
                .key(outputKey)
                .contentType("text/csv")
                .build();

        s3.putObject(putReq, RequestBody.fromString(out.toString(), StandardCharsets.UTF_8));

        System.out.println("Export complete: s3://" + bucket + "/" + outputKey);
    }
}
