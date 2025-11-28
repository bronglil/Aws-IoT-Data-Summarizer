package fr.trafficsolution.util;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Key used to group flows by:
 *   - Source IP
 *   - Destination IP
 *   - Date (day)
 */
public class SummaryKey {

    private final String srcIp;
    private final String dstIp;
    private final LocalDate date;

    public SummaryKey(String srcIp, String dstIp, LocalDate date) {
        this.srcIp = srcIp;
        this.dstIp = dstIp;
        this.date = date;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public String getDstIp() {
        return dstIp;
    }

    public LocalDate getDate() {
        return date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SummaryKey)) return false;
        SummaryKey that = (SummaryKey) o;
        return Objects.equals(srcIp, that.srcIp)
                && Objects.equals(dstIp, that.dstIp)
                && Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcIp, dstIp, date);
    }

    @Override
    public String toString() {
        return "SummaryKey{" +
                "srcIp='" + srcIp + '\'' +
                ", dstIp='" + dstIp + '\'' +
                ", date=" + date +
                '}';
    }
}
