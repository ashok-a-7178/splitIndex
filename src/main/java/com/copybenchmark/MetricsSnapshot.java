package com.copybenchmark;

/**
 * Immutable point-in-time sample of system-level metrics.
 */
public final class MetricsSnapshot {

    /** Wall-clock time when the sample was taken. */
    public final long timestampMs;

    /** JVM process CPU load (0.0 - 1.0). */
    public final double processCpuLoad;

    /** Whole-system CPU load (0.0 - 1.0). */
    public final double systemCpuLoad;

    /** Heap memory currently in use (bytes). */
    public final long heapUsedBytes;

    /** Maximum heap memory available (bytes). */
    public final long heapMaxBytes;

    /** Cumulative bytes read from disk (from /proc/self/io). */
    public final long ioReadBytes;

    /** Cumulative bytes written to disk (from /proc/self/io). */
    public final long ioWriteBytes;

    public MetricsSnapshot(long timestampMs,
                           double processCpuLoad,
                           double systemCpuLoad,
                           long heapUsedBytes,
                           long heapMaxBytes,
                           long ioReadBytes,
                           long ioWriteBytes) {
        this.timestampMs = timestampMs;
        this.processCpuLoad = processCpuLoad;
        this.systemCpuLoad = systemCpuLoad;
        this.heapUsedBytes = heapUsedBytes;
        this.heapMaxBytes = heapMaxBytes;
        this.ioReadBytes = ioReadBytes;
        this.ioWriteBytes = ioWriteBytes;
    }

    @Override
    public String toString() {
        return String.format(
                "MetricsSnapshot{ts=%d, pCPU=%.2f%%, sCPU=%.2f%%, heap=%.1fMB, ioR=%.1fMB, ioW=%.1fMB}",
                timestampMs,
                processCpuLoad * 100,
                systemCpuLoad * 100,
                heapUsedBytes / (1024.0 * 1024.0),
                ioReadBytes / (1024.0 * 1024.0),
                ioWriteBytes / (1024.0 * 1024.0));
    }
}
