package fr.trafficsolution.consolidator;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.event.S3EventNotification;

import fr.trafficsolution.util.SummaryKey;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

public class ConsolidateWorker implements RequestHandler<SQSEvent, String> {

    private final S3Client s3Client = S3Client.create();

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        LambdaLogger log = context.getLogger();

        if (event.getRecords() == null || event.getRecords().isEmpty()) {
            log.log("Consolidator - No SQS records\n");
            return "NO_RECORDS";
        }

        // Single message (batch size = 1)
        SQSEvent.SQSMessage msg = event.getRecords().get(0);
        String body = msg.getBody();

        // Extract bucket from original S3 event
        S3EventNotification s3Event = S3EventNotification.parseJson(body);
        String bucketName = s3Event.getRecords().get(0).getS3().getBucket().getName();

        log.log("Consolidator - Triggered for bucket: " + bucketName + "\n");

        try {
            Map<SummaryKey, Stats> aggregated = new HashMap<>();

            // List summary files
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix("summaries/")
                    .build();

            ListObjectsV2Response listRes = s3Client.listObjectsV2(listReq);

            for (S3Object obj : listRes.contents()) {
                String key = obj.key();
                if (!key.endsWith(".csv")) continue;
                if (key.equals("summaries/")) continue;

                log.log("Consolidator - Reading summary file: " + key + "\n");

                ResponseBytes<GetObjectResponse> bytes =
                        s3Client.getObjectAsBytes(GetObjectRequest.builder()
                                .bucket(bucketName)
                                .key(key)
                                .build());

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                new ByteArrayInputStream(bytes.asByteArray()), StandardCharsets.UTF_8))) {

                    String line;
                    boolean first = true;

                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        // Skip header
                        if (first) {
                            first = false;
                            if (line.startsWith("date,")) {
                                continue;
                            }
                        }

                        String[] c = line.split(",", -1);
                        if (c.length < 5) {
                            log.log("Consolidator - Skipping malformed line: " + line + "\n");
                            continue;
                        }

                        // date,src_ip,dst_ip,total_flow_duration,total_fwd_pkts
                        LocalDate date = LocalDate.parse(c[0].trim());
                        String src = c[1].trim();
                        String dst = c[2].trim();
                        double totalDur = Double.parseDouble(c[3].trim());
                        long totalPkts = Long.parseLong(c[4].trim());

                        SummaryKey keyObj = new SummaryKey(src, dst, date);
                        Stats st = aggregated.get(keyObj);
                        if (st == null) {
                            st = new Stats();
                            aggregated.put(keyObj, st);
                        }

                        st.add(totalDur, totalPkts);
                    }
                }
            }

            log.log("Consolidator - Aggregated keys: " + aggregated.size() + "\n");

            // Build consolidated CSV
            String consolidatedCsv = buildConsolidatedCsv(aggregated);

            String consolidatedKey = "consolidated/data_consolidated.csv";

            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(consolidatedKey)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putReq, RequestBody.fromBytes(consolidatedCsv.getBytes(StandardCharsets.UTF_8)));

            log.log("Consolidator - Wrote consolidated file: s3://" + bucketName + "/" + consolidatedKey + "\n");

            return "OK";

        } catch (Exception e) {
            log.log("Consolidator - ERROR: " + e.getMessage() + "\n");
            throw new RuntimeException(e);
        }
    }

    /**
     * CSV header:
     *   date,src_ip,dst_ip,avg_duration,stddev_duration,avg_pkts,stddev_pkts
     */
    private String buildConsolidatedCsv(Map<SummaryKey, Stats> aggregated) {
        StringBuilder sb = new StringBuilder();
        sb.append("date,src_ip,dst_ip,avg_duration,stddev_duration,avg_pkts,stddev_pkts\n");

        for (Map.Entry<SummaryKey, Stats> entry : aggregated.entrySet()) {
            SummaryKey key = entry.getKey();
            Stats st = entry.getValue();

            sb.append(key.getDate()).append(',')
              .append(key.getSrcIp()).append(',')
              .append(key.getDstIp()).append(',')
              .append(st.getAvgDuration()).append(',')
              .append(st.getStdDevDuration()).append(',')
              .append(st.getAvgPkts()).append(',')
              .append(st.getStdDevPkts()).append('\n');
        }

        return sb.toString();
    }

    /**
     * Online stats for mean and standard deviation (per key).
     */
    private static class Stats {
        private int count = 0;
        private double sumDur = 0.0;
        private double sumDurSq = 0.0;
        private double sumPkts = 0.0;
        private double sumPktsSq = 0.0;

        void add(double totalDuration, long totalPkts) {
            count++;
            sumDur += totalDuration;
            sumDurSq += totalDuration * totalDuration;
            sumPkts += totalPkts;
            sumPktsSq += (double) totalPkts * (double) totalPkts;
        }

        double getAvgDuration() {
            return count == 0 ? 0.0 : sumDur / count;
        }

        double getStdDevDuration() {
            if (count <= 1) return 0.0;
            double mean = sumDur / count;
            double meanSq = sumDurSq / count;
            double variance = meanSq - mean * mean;
            return variance <= 0 ? 0.0 : Math.sqrt(variance);
        }

        double getAvgPkts() {
            return count == 0 ? 0.0 : sumPkts / count;
        }

        double getStdDevPkts() {
            if (count <= 1) return 0.0;
            double mean = sumPkts / count;
            double meanSq = sumPktsSq / count;
            double variance = meanSq - mean * mean;
            return variance <= 0 ? 0.0 : Math.sqrt(variance);
        }
    }
}
