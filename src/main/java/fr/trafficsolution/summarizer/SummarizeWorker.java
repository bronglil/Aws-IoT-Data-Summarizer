package fr.trafficsolution.summarizer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

import fr.trafficsolution.util.CSVParser;
import fr.trafficsolution.util.SummaryKey;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

public class SummarizeWorker implements RequestHandler<SQSEvent, String> {

    private final S3Client s3Client = S3Client.create();
    private final CSVParser csvParser = new CSVParser();
    private final SqsClient sqsClient = SqsClient.create();

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        LambdaLogger log = context.getLogger();

        if (event.getRecords() == null || event.getRecords().isEmpty()) {
            log.log("SummarizeWorker - No SQS records\n");
            return "NO_RECORDS";
        }

        // We process only the first message for now (batch size = 1 in trigger)
        SQSEvent.SQSMessage msg = event.getRecords().get(0);
        String body = msg.getBody();

        // Body contains the original S3 event JSON
        S3EventNotification s3Event = S3EventNotification.parseJson(body);
        S3EventNotificationRecord record = s3Event.getRecords().get(0);

        String bucketName = record.getS3().getBucket().getName();
        String objectKey = URLDecoder.decode(
                record.getS3().getObject().getKey(),
                StandardCharsets.UTF_8
        );

        log.log("SummarizeWorker - Processing file: s3://" + bucketName + "/" + objectKey + "\n");

        try {
            // 1) Download raw CSV from S3
            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            ResponseBytes<GetObjectResponse> rawBytes = s3Client.getObjectAsBytes(getReq);

            List<String[]> rows;
            try (InputStream in = new ByteArrayInputStream(rawBytes.asByteArray())) {
                rows = csvParser.parse(in);
            }

            log.log("SummarizeWorker - Parsed " + rows.size() + " data rows\n");

            // 2) Summarize per (src,dst,date)
            Map<SummaryKey, SummaryValue> summaries = summarize(rows);

            log.log("SummarizeWorker - Computed " + summaries.size() + " summary groups\n");

            // 3) Build summary CSV
            String summaryCsv = buildSummaryCsv(summaries);

            // 4) Write summary to summaries/
            String summaryKey = "summaries/"
                    + objectKey.replaceFirst("^incoming/", "")
                               .replaceAll("\\.csv$", "")   // strip .csv
                    + "_summary.csv";

            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(summaryKey)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putReq, RequestBody.fromBytes(summaryCsv.getBytes(StandardCharsets.UTF_8)));

            log.log("SummarizeWorker - Wrote summary: s3://" + bucketName + "/" + summaryKey + "\n");

            // 5) Notify consolidator via SQS
            String consolidatorQueueUrl = System.getenv("CONSOLIDATOR_QUEUE_URL");
            if (consolidatorQueueUrl == null || consolidatorQueueUrl.isEmpty()) {
                log.log("SummarizeWorker - WARNING: CONSOLIDATOR_QUEUE_URL is not set\n");
            } else {
                sqsClient.sendMessage(
                        SendMessageRequest.builder()
                                .queueUrl(consolidatorQueueUrl)
                                // forward the same S3 event JSON so consolidator knows the bucket
                                .messageBody(body)
                                .build()
                );
                log.log("SummarizeWorker - Sent message to consolidator queue\n");
            }

            return "OK";

        } catch (Exception e) {
            log.log("SummarizeWorker - ERROR: " + e.getMessage() + "\n");
            throw new RuntimeException(e);
        }
    }

    /**
     * Summarize rows per (src_ip, dst_ip, date) by:
     *  - summing Flow Duration (double)
     *  - summing Tot Fwd Pkts (long)
     */
    private Map<SummaryKey, SummaryValue> summarize(List<String[]> rows) {
        Map<SummaryKey, SummaryValue> result = new HashMap<>();

        final int COL_SRC_IP        = 1;
        final int COL_DST_IP        = 3;
        final int COL_TIMESTAMP     = 6;
        final int COL_FLOW_DURATION = 7;
        final int COL_TOT_FWD_PKTS  = 8;

        for (String[] r : rows) {
            if (r == null) continue;
            // Ensure the row has at least up to Tot Fwd Pkts column
            if (r.length <= COL_TOT_FWD_PKTS) {
                continue;
            }

            try {
                String src = r[COL_SRC_IP].trim();
                String dst = r[COL_DST_IP].trim();
                String timestamp = r[COL_TIMESTAMP].trim();
                String durStr = r[COL_FLOW_DURATION].trim();
                String pktsStr = r[COL_TOT_FWD_PKTS].trim();

                if (src.isEmpty() || dst.isEmpty()
                        || timestamp.isEmpty()
                        || durStr.isEmpty()
                        || pktsStr.isEmpty()) {
                    continue;
                }

                LocalDate date = csvParser.parseDate(timestamp);

                double duration = Double.parseDouble(durStr);
                long pkts = Long.parseLong(pktsStr);

                SummaryKey key = new SummaryKey(src, dst, date);
                SummaryValue agg = result.get(key);
                if (agg == null) {
                    agg = new SummaryValue();
                    result.put(key, agg);
                }

                agg.addDuration(duration);
                agg.addForwardPkts(pkts);

            } catch (Exception ignored) {
                // skip bad rows
            }
        }

        return result;
    }

    /**
     * CSV header:
     *   date,src_ip,dst_ip,total_flow_duration,total_fwd_pkts
     */
    private String buildSummaryCsv(Map<SummaryKey, SummaryValue> summaries) {
        StringBuilder sb = new StringBuilder();
        sb.append("date,src_ip,dst_ip,total_flow_duration,total_fwd_pkts\n");

        // No strict requirement to sort, but you can if you want
        for (Map.Entry<SummaryKey, SummaryValue> entry : summaries.entrySet()) {
            SummaryKey key = entry.getKey();
            SummaryValue v = entry.getValue();

            sb.append(key.getDate()).append(',')
              .append(key.getSrcIp()).append(',')
              .append(key.getDstIp()).append(',')
              .append(v.getTotalFlowDuration()).append(',')
              .append(v.getTotalForwardPkts()).append('\n');
        }

        return sb.toString();
    }

    private static class SummaryValue {
        private double totalFlowDuration;
        private long totalForwardPkts;

        void addDuration(double d) { totalFlowDuration += d; }
        void addForwardPkts(long p) { totalForwardPkts += p; }

        double getTotalFlowDuration() { return totalFlowDuration; }
        long getTotalForwardPkts() { return totalForwardPkts; }
    }
}
