package fr.trafficsolution.util;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * CSVParser - Utility class for CSV parsing operations.
 */
public class CSVParser {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    /**
     * Parses CSV content from bytes and returns list of records.
     */
    public List<String[]> parseCsv(byte[] csvBytes) throws IOException, CsvException {
        try (InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(csvBytes),
                StandardCharsets.UTF_8);
                CSVReader csvReader = new CSVReader(reader)) {

            List<String[]> records = csvReader.readAll();

            // Skip header if present
            if (!records.isEmpty() && isHeader(records.get(0))) {
                records.remove(0);
            }

            return records;
        }
    }

    /**
     * Parses CSV content from InputStream.
     */
    public List<String[]> parseCsv(InputStream inputStream) throws IOException, CsvException {
        byte[] bytes = inputStream.readAllBytes();
        return parseCsv(bytes);
    }

    /**
     * Writes records to CSV format as bytes.
     */
    public byte[] writeCsv(List<String[]> records, String[] header) throws IOException {
        StringWriter stringWriter = new StringWriter();

        try (CSVWriter csvWriter = new CSVWriter(stringWriter)) {
            // Write header if provided
            if (header != null) {
                csvWriter.writeNext(header);
            }

            // Write records
            for (String[] record : records) {
                csvWriter.writeNext(record);
            }
        }

        return stringWriter.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Checks if a row is a header row.
     */
    public boolean isHeader(String[] row) {
        if (row == null || row.length == 0) {
            return false;
        }
        String firstCell = row[0].toLowerCase();
        return firstCell.contains("src") || firstCell.contains("source") ||
                firstCell.contains("date") || firstCell.contains("dst") ||
                firstCell.contains("destination");
    }

    /**
     * Parses a date string using ISO_LOCAL_DATE format.
     */
    public LocalDate parseDate(String dateStr) {
        return LocalDate.parse(dateStr, DATE_FORMATTER);
    }

    /**
     * Formats a date using ISO_LOCAL_DATE format.
     */
    public String formatDate(LocalDate date) {
        return date.format(DATE_FORMATTER);
    }

    /**
     * Gets the date formatter.
     */
    public DateTimeFormatter getDateFormatter() {
        return DATE_FORMATTER;
    }
}
