package fr.trafficsolution.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Simple CSV parser for the IoT dataset.
 *
 * Assumptions:
 *  - First line is a header (starts with "Flow ID") and is skipped.
 *  - Separator is a comma ','.
 *  - No quoted fields with embedded commas.
 *
 * Example row:
 *  Flow ID,Src IP,Src Port,Dst IP,Dst Port,Protocol,Timestamp,Flow Duration,Tot Fwd Pkts,...
 */
public class CSVParser {

    // Example timestamp string: "06/12/2022 11:00:15 PM"
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a", Locale.US);

    /**
     * Parse the CSV from an InputStream into a list of String[] rows (no header).
     */
    public List<String[]> parse(InputStream inputStream) throws IOException {
        List<String[]> rows = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {

            String line;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // Skip header line
                if (isFirstLine) {
                    isFirstLine = false;
                    if (line.startsWith("Flow ID")) {
                        // It's the header, skip it
                        continue;
                    }
                }

                // Split by comma, keep empty strings for missing values
                String[] cols = line.split(",", -1);
                rows.add(cols);
            }
        }

        return rows;
    }

    /**
     * Parse a full timestamp string (e.g., "06/12/2022 11:00:15 PM")
     * and return only the date portion.
     */
    public LocalDate parseDate(String timestamp) {
        LocalDateTime dateTime = LocalDateTime.parse(timestamp, TIMESTAMP_FORMATTER);
        return dateTime.toLocalDate();
    }
}
