package fr.trafficsolution.summarizer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import fr.trafficsolution.util.CSVParser;
import fr.trafficsolution.util.S3Helper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SummarizeWorker - Lambda handler that:
 * 1. Reads SQS message containing S3 file location
 * 2. Downloads CSV file from S3
 * 3. Summarizes data per (Src, Dst, Date)
 * 4. Writes summary file to S3 summaries/
 * 5. Sends message to consolidator-requests SQS queue
 */
public class SummarizeWorker implements RequestHandler<SQSEvent, String> {
    private final S3Helper s3Helper;
    private final CSVParser csvParser;
    private final SqsClient sqsClient;

    // Environment variables (set in Lambda configuration)
    private final String bucketName;
    private final String consolidatorQueueUrl;

    public SummarizeWorker() {
        this.s3Helper = new S3Helper();
        this.csvParser = new CSVParser();
        this.sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        // Get configuration from environment variables
        this.bucketName = System.getenv("BUCKET_NAME");
        this.consolidatorQueueUrl = System.getenv("CONSOLIDATOR_QUEUE_URL");
    }

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Received " + event.getRecords().size() + " SQS message(s)");

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                String s3FileLocation = message.getBody();
                context.getLogger().log("Processing S3 file: " + s3FileLocation);

                // Parse S3 location (format: bucket/key or s3://bucket/key)
                S3Helper.S3Location location = s3Helper.parseLocation(s3FileLocation);
                String inputKey = location.key;
                String bucket = location.bucket != null ? location.bucket : bucketName;

                // Generate output summary key
                String outputKey = "summaries/" + extractFileName(inputKey) + "_summary.csv";

                // Process the file
                processFile(bucket, inputKey, outputKey, context);

                // Send message to consolidator queue
                sendToConsolidatorQueue(bucket, outputKey, context);

                context.getLogger().log("Successfully processed: " + inputKey);

            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Failed to process SQS message", e);
            }
        }

        return "Processed " + event.getRecords().size() + " message(s)";
    }

    /**
     * Processes a CSV file: reads from S3, computes summaries, writes to S3.
     */
    private void processFile(String bucket, String inputKey, String outputKey, Context context)
            throws IOException, CsvException {

        context.getLogger().log("Reading CSV from: s3://" + bucket + "/" + inputKey);

        // Read CSV from S3 using S3Helper
        byte[] csvBytes = s3Helper.readObjectAsBytes(bucket, inputKey);
        List<String[]> records = csvParser.parseCsv(csvBytes);
        context.getLogger().log("Read " + records.size() + " records");

        // Compute summaries per (Src, Dst, date)
        Map<SummaryKey, Double> summaries = computeSummaries(records);
        context.getLogger().log("Computed " + summaries.size() + " unique summaries");

        // Write summary to S3
        writeSummaryToS3(bucket, outputKey, summaries);
        context.getLogger().log("Wrote summary to: s3://" + bucket + "/" + outputKey);
    }

    /**
     * Computes totals per (Src, Dst, date) combination.
     * Expected CSV format: Src, Dst, Date, Value (or similar)
     */
    private Map<SummaryKey, Double> computeSummaries(List<String[]> records) {
        Map<SummaryKey, Double> summaries = new HashMap<>();

        for (String[] record : records) {
            if (record.length < 3) {
                System.err.println("Skipping invalid record: " + Arrays.toString(record));
                continue;
            }

            try {
                String src = record[0].trim();
                String dst = record[1].trim();
                String dateStr = record[2].trim();
                LocalDate date = csvParser.parseDate(dateStr);

                // Assume the value is in the 4th column (index 3), or default to 1.0
                double value = record.length > 3 ? Double.parseDouble(record[3].trim()) : 1.0;

                SummaryKey key = new SummaryKey(src, dst, date);
                summaries.merge(key, value, Double::sum);

            } catch (Exception e) {
                System.err.println("Error processing record: " + Arrays.toString(record) +
                        " - " + e.getMessage());
            }
        }

        return summaries;
    }

    /**
     * Writes the summary map to S3 as a CSV file.
     */
    private void writeSummaryToS3(String bucketName, String key,
            Map<SummaryKey, Double> summaries) throws IOException {

        StringWriter stringWriter = new StringWriter();

        try (CSVWriter csvWriter = new CSVWriter(stringWriter)) {
            // Write header
            csvWriter.writeNext(new String[] { "Src", "Dst", "Date", "Total" });

            // Write summaries sorted by key
            List<SummaryKey> sortedKeys = summaries.keySet().stream()
                    .sorted(Comparator
                            .comparing(SummaryKey::getSrc)
                            .thenComparing(SummaryKey::getDst)
                            .thenComparing(SummaryKey::getDate))
                    .collect(Collectors.toList());

            for (SummaryKey summaryKey : sortedKeys) {
                csvWriter.writeNext(new String[] {
                        summaryKey.getSrc(),
                        summaryKey.getDst(),
                        csvParser.formatDate(summaryKey.getDate()),
                        String.valueOf(summaries.get(summaryKey))
                });
            }
        }

        // Upload to S3 using S3Helper
        byte[] content = stringWriter.toString().getBytes(StandardCharsets.UTF_8);
        s3Helper.writeObject(bucketName, key, content);
    }

    /**
     * Sends a message to the consolidator queue with the summary file location.
     */
    private void sendToConsolidatorQueue(String bucket, String summaryKey, Context context) {
        if (consolidatorQueueUrl == null || consolidatorQueueUrl.isEmpty()) {
            context.getLogger().log("WARNING: CONSOLIDATOR_QUEUE_URL not set, skipping SQS message");
            return;
        }

        String messageBody = bucket + "/" + summaryKey;

        SendMessageRequest sendRequest = SendMessageRequest.builder()
                .queueUrl(consolidatorQueueUrl)
                .messageBody(messageBody)
                .build();

        sqsClient.sendMessage(sendRequest);
        context.getLogger().log("Sent message to consolidator queue: " + messageBody);
    }

    /**
     * Extracts filename from S3 key.
     */
    private String extractFileName(String key) {
        int lastSlash = key.lastIndexOf('/');
        if (lastSlash >= 0) {
            String filename = key.substring(lastSlash + 1);
            // Remove extension if present
            int lastDot = filename.lastIndexOf('.');
            if (lastDot > 0) {
                return filename.substring(0, lastDot);
            }
            return filename;
        }
        return key;
    }

    /**
     * Inner class to represent a summary key (Src, Dst, Date).
     */
    public static class SummaryKey {
        private final String src;
        private final String dst;
        private final LocalDate date;

        public SummaryKey(String src, String dst, LocalDate date) {
            this.src = src;
            this.dst = dst;
            this.date = date;
        }

        public String getSrc() {
            return src;
        }

        public String getDst() {
            return dst;
        }

        public LocalDate getDate() {
            return date;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            SummaryKey that = (SummaryKey) o;
            return Objects.equals(src, that.src) &&
                    Objects.equals(dst, that.dst) &&
                    Objects.equals(date, that.date);
        }

        @Override
        public int hashCode() {
            return Objects.hash(src, dst, date);
        }

        @Override
        public String toString() {
            return "SummaryKey{" +
                    "src='" + src + '\'' +
                    ", dst='" + dst + '\'' +
                    ", date=" + date +
                    '}';
        }
    }
}
