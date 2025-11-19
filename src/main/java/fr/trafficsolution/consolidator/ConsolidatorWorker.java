package fr.trafficsolution.consolidator;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencsv.CSVWriter;
import fr.trafficsolution.summarizer.SummarizeWorker;
import fr.trafficsolution.util.CSVParser;
import fr.trafficsolution.util.S3Helper;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ConsolidatorWorker - Lambda handler that:
 * 1. Receives SQS message containing summary file location
 * 2. Reads summary from S3
 * 3. Updates aggregated stats (avg, stddev, etc.)
 * 4. Writes updated aggregated JSON/CSV to consolidated/
 */
public class ConsolidatorWorker implements RequestHandler<SQSEvent, String> {
    private final S3Helper s3Helper;
    private final CSVParser csvParser;

    // Environment variables (set in Lambda configuration)
    private final String bucketName;

    public ConsolidatorWorker() {
        this.s3Helper = new S3Helper();
        this.csvParser = new CSVParser();
        this.bucketName = System.getenv("BUCKET_NAME");
    }

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Received " + event.getRecords().size() + " SQS message(s)");

        // Load existing aggregates from S3
        Map<SummarizeWorker.SummaryKey, AggregateStats> aggregates = loadExistingAggregates(context);

        // Process each message
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                String summaryLocation = message.getBody();
                context.getLogger().log("Processing summary file: " + summaryLocation);

                // Parse S3 location using S3Helper
                S3Helper.S3Location location = s3Helper.parseLocation(summaryLocation);
                String summaryKey = location.key;
                String bucket = location.bucket != null ? location.bucket : bucketName;

                // Read and merge summary
                Map<SummarizeWorker.SummaryKey, Double> summary = readSummaryFromS3(bucket, summaryKey);
                context.getLogger().log("Read " + summary.size() + " summary entries");

                // Update aggregates
                updateAggregates(aggregates, summary);

            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Write updated aggregates to S3 (both CSV and JSON)
        String timestamp = String.valueOf(System.currentTimeMillis());
        try {
            writeAggregatesToS3(aggregates, timestamp, context);
        } catch (IOException e) {
            context.getLogger().log("Error writing aggregates: " + e.getMessage());
            throw new RuntimeException("Failed to write aggregates", e);
        }

        context.getLogger().log("Consolidation completed. Total unique keys: " + aggregates.size());
        return "Processed " + event.getRecords().size() + " message(s)";
    }

    /**
     * Loads existing aggregates from S3 if they exist.
     */
    private Map<SummarizeWorker.SummaryKey, AggregateStats> loadExistingAggregates(Context context) {
        Map<SummarizeWorker.SummaryKey, AggregateStats> aggregates = new HashMap<>();

        try {
            // Try to load the most recent consolidated file
            String latestKey = "consolidated/latest_aggregates.json";

            try {
                String json = s3Helper.readObjectAsString(bucketName, latestKey);

                Gson gson = new Gson();
                AggregateData data = gson.fromJson(json, AggregateData.class);

                for (AggregateEntry entry : data.aggregates) {
                    SummarizeWorker.SummaryKey key = new SummarizeWorker.SummaryKey(
                            entry.src, entry.dst, csvParser.parseDate(entry.date));
                    aggregates.put(key, new AggregateStats(entry.count, entry.sum, entry.sumSquares));
                }

                context.getLogger().log("Loaded " + aggregates.size() + " existing aggregates");
            } catch (Exception e) {
                context.getLogger().log("No existing aggregates found, starting fresh");
            }
        } catch (Exception e) {
            context.getLogger().log("Could not load existing aggregates: " + e.getMessage());
        }

        return aggregates;
    }

    /**
     * Reads a summary CSV file from S3.
     */
    private Map<SummarizeWorker.SummaryKey, Double> readSummaryFromS3(String bucketName, String key)
            throws IOException, com.opencsv.exceptions.CsvException {

        Map<SummarizeWorker.SummaryKey, Double> summary = new HashMap<>();

        // Read CSV using S3Helper and CSVParser
        byte[] csvBytes = s3Helper.readObjectAsBytes(bucketName, key);
        List<String[]> records = csvParser.parseCsv(csvBytes);

        if (records.isEmpty()) {
            return summary;
        }

        // Check if first row is header
        int startIndex = 0;
        if (csvParser.isHeader(records.get(0))) {
            startIndex = 1;
        }

        // Parse records
        for (int i = startIndex; i < records.size(); i++) {
            String[] record = records.get(i);

            if (record.length < 4) {
                System.err.println("Skipping invalid record in " + key + ": " + Arrays.toString(record));
                continue;
            }

            try {
                String src = record[0].trim();
                String dst = record[1].trim();
                String dateStr = record[2].trim();
                double total = Double.parseDouble(record[3].trim());

                LocalDate date = csvParser.parseDate(dateStr);
                SummarizeWorker.SummaryKey summaryKey = new SummarizeWorker.SummaryKey(src, dst, date);

                summary.put(summaryKey, total);

            } catch (Exception e) {
                System.err.println("Error processing record in " + key + ": " +
                        Arrays.toString(record) + " - " + e.getMessage());
            }
        }

        return summary;
    }

    /**
     * Updates aggregates with new summary data.
     */
    private void updateAggregates(Map<SummarizeWorker.SummaryKey, AggregateStats> aggregates,
            Map<SummarizeWorker.SummaryKey, Double> summary) {
        for (Map.Entry<SummarizeWorker.SummaryKey, Double> entry : summary.entrySet()) {
            SummarizeWorker.SummaryKey key = entry.getKey();
            double value = entry.getValue();

            aggregates.compute(key, (k, stats) -> {
                if (stats == null) {
                    return new AggregateStats(1, value, value * value);
                } else {
                    return stats.add(value);
                }
            });
        }
    }

    /**
     * Writes aggregates to S3 in both CSV and JSON formats.
     */
    private void writeAggregatesToS3(Map<SummarizeWorker.SummaryKey, AggregateStats> aggregates,
            String timestamp, Context context) throws IOException {

        // Write CSV
        String csvKey = "consolidated/aggregates_" + timestamp + ".csv";
        writeAggregatesCsv(bucketName, csvKey, aggregates);
        context.getLogger().log("Wrote CSV to: s3://" + bucketName + "/" + csvKey);

        // Write JSON
        String jsonKey = "consolidated/aggregates_" + timestamp + ".json";
        writeAggregatesJson(bucketName, jsonKey, aggregates);
        context.getLogger().log("Wrote JSON to: s3://" + bucketName + "/" + jsonKey);

        // Write latest JSON (for next consolidation)
        String latestKey = "consolidated/latest_aggregates.json";
        writeAggregatesJson(bucketName, latestKey, aggregates);
    }

    /**
     * Writes aggregates as CSV.
     */
    private void writeAggregatesCsv(String bucketName, String key,
            Map<SummarizeWorker.SummaryKey, AggregateStats> aggregates) throws IOException {

        StringWriter stringWriter = new StringWriter();

        try (CSVWriter csvWriter = new CSVWriter(stringWriter)) {
            // Write header
            csvWriter.writeNext(new String[] {
                    "Src", "Dst", "Date", "Count", "Sum", "Average", "StdDev"
            });

            // Write aggregates sorted by key
            List<SummarizeWorker.SummaryKey> sortedKeys = aggregates.keySet().stream()
                    .sorted(Comparator
                            .comparing(SummarizeWorker.SummaryKey::getSrc)
                            .thenComparing(SummarizeWorker.SummaryKey::getDst)
                            .thenComparing(SummarizeWorker.SummaryKey::getDate))
                    .collect(Collectors.toList());

            for (SummarizeWorker.SummaryKey summaryKey : sortedKeys) {
                AggregateStats stats = aggregates.get(summaryKey);
                csvWriter.writeNext(new String[] {
                        summaryKey.getSrc(),
                        summaryKey.getDst(),
                        csvParser.formatDate(summaryKey.getDate()),
                        String.valueOf(stats.count),
                        String.valueOf(stats.sum),
                        String.valueOf(stats.getAverage()),
                        String.valueOf(stats.getStdDev())
                });
            }
        }

        // Upload to S3 using S3Helper
        byte[] content = stringWriter.toString().getBytes(StandardCharsets.UTF_8);
        s3Helper.writeObject(bucketName, key, content);
    }

    /**
     * Writes aggregates as JSON.
     */
    private void writeAggregatesJson(String bucketName, String key,
            Map<SummarizeWorker.SummaryKey, AggregateStats> aggregates) throws IOException {

        AggregateData data = new AggregateData();
        data.aggregates = new ArrayList<>();

        for (Map.Entry<SummarizeWorker.SummaryKey, AggregateStats> entry : aggregates.entrySet()) {
            AggregateEntry aggEntry = new AggregateEntry();
            aggEntry.src = entry.getKey().getSrc();
            aggEntry.dst = entry.getKey().getDst();
            aggEntry.date = csvParser.formatDate(entry.getKey().getDate());
            aggEntry.count = entry.getValue().count;
            aggEntry.sum = entry.getValue().sum;
            aggEntry.sumSquares = entry.getValue().sumSquares;
            aggEntry.average = entry.getValue().getAverage();
            aggEntry.stdDev = entry.getValue().getStdDev();

            data.aggregates.add(aggEntry);
        }

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(data);

        // Upload to S3 using S3Helper
        s3Helper.writeObject(bucketName, key, json);
    }

    /**
     * Class to track aggregate statistics.
     */
    private static class AggregateStats {
        long count;
        double sum;
        double sumSquares; // Sum of squares for stddev calculation

        AggregateStats(long count, double sum, double sumSquares) {
            this.count = count;
            this.sum = sum;
            this.sumSquares = sumSquares;
        }

        AggregateStats add(double value) {
            return new AggregateStats(
                    this.count + 1,
                    this.sum + value,
                    this.sumSquares + (value * value));
        }

        double getAverage() {
            return count > 0 ? sum / count : 0.0;
        }

        double getStdDev() {
            if (count <= 1) {
                return 0.0;
            }
            double mean = getAverage();
            double variance = (sumSquares / count) - (mean * mean);
            return Math.sqrt(Math.max(0, variance));
        }
    }

    /**
     * JSON data structure for aggregates.
     */
    private static class AggregateData {
        List<AggregateEntry> aggregates;
    }

    /**
     * JSON entry for a single aggregate.
     */
    private static class AggregateEntry {
        String src;
        String dst;
        String date;
        long count;
        double sum;
        double sumSquares;
        double average;
        double stdDev;
    }
}
