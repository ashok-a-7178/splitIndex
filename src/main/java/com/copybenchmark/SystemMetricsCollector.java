package com.copybenchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background daemon thread that periodically samples system-level metrics
 * (CPU load, heap memory, disk IO bytes) and stores them as
 * {@link MetricsSnapshot} instances for later analysis.
 *
 * <p><strong>Note:</strong> This class requires a Sun/Oracle/OpenJDK JVM
 * because it casts to {@code com.sun.management.OperatingSystemMXBean} to
 * obtain process-level CPU load.  It will not work on non-HotSpot JVMs.</p>
 *
 * <p>Usage:
 * <pre>{@code
 *   SystemMetricsCollector collector = new SystemMetricsCollector("source");
 *   collector.start();
 *   // ... do work ...
 *   collector.stop();
 *   collector.printSummary();
 * }</pre>
 */
public class SystemMetricsCollector {

    private final String label;
    private final long intervalMs;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread samplerThread;

    private final List<MetricsSnapshot> snapshots =
            Collections.synchronizedList(new ArrayList<MetricsSnapshot>());

    private final com.sun.management.OperatingSystemMXBean osMxBean;
    private final MemoryMXBean memoryMxBean;

    public SystemMetricsCollector(String label) {
        this(label, CopyBenchmarkConfig.METRICS_SAMPLE_INTERVAL_MS);
    }

    public SystemMetricsCollector(String label, long intervalMs) {
        this.label = label;
        this.intervalMs = intervalMs;
        this.osMxBean = (com.sun.management.OperatingSystemMXBean)
                ManagementFactory.getOperatingSystemMXBean();
        this.memoryMxBean = ManagementFactory.getMemoryMXBean();
    }

    /* ------------------------------------------------------------------ */
    /*  Lifecycle                                                          */
    /* ------------------------------------------------------------------ */

    /** Start the background sampling thread. */
    public void start() {
        if (running.getAndSet(true)) {
            return; // already running
        }
        snapshots.clear();
        samplerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    snapshots.add(sample());
                    try {
                        Thread.sleep(intervalMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }, "metrics-" + label);
        samplerThread.setDaemon(true);
        samplerThread.start();
    }

    /** Stop sampling and wait for the thread to finish. */
    public void stop() {
        running.set(false);
        if (samplerThread != null) {
            try {
                samplerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Sampling                                                           */
    /* ------------------------------------------------------------------ */

    private MetricsSnapshot sample() {
        double cpuLoad = osMxBean.getProcessCpuLoad();
        double systemCpuLoad = osMxBean.getSystemCpuLoad();

        MemoryUsage heap = memoryMxBean.getHeapMemoryUsage();
        long heapUsedBytes = heap.getUsed();
        long heapMaxBytes = heap.getMax();

        long[] io = readProcIO();   // [readBytes, writeBytes]

        return new MetricsSnapshot(
                System.currentTimeMillis(),
                cpuLoad,
                systemCpuLoad,
                heapUsedBytes,
                heapMaxBytes,
                io[0],
                io[1]
        );
    }

    /**
     * Reads {@code /proc/self/io} on Linux to obtain cumulative read/write
     * byte counts. Returns {@code [0, 0]} if the file is unavailable.
     */
    private long[] readProcIO() {
        File procIo = new File("/proc/self/io");
        long readBytes = 0;
        long writeBytes = 0;
        if (procIo.exists()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(procIo));
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith("read_bytes:")) {
                            readBytes = Long.parseLong(line.split("\\s+")[1]);
                        } else if (line.startsWith("write_bytes:")) {
                            writeBytes = Long.parseLong(line.split("\\s+")[1]);
                        }
                    }
                } finally {
                    br.close();
                }
            } catch (IOException ignored) {
                // /proc/self/io may not be available on all platforms
            }
        }
        return new long[]{readBytes, writeBytes};
    }

    /* ------------------------------------------------------------------ */
    /*  Results                                                            */
    /* ------------------------------------------------------------------ */

    /** Return an unmodifiable view of collected snapshots. */
    public List<MetricsSnapshot> getSnapshots() {
        return Collections.unmodifiableList(new ArrayList<MetricsSnapshot>(snapshots));
    }

    /** Pretty-print a summary of the collected metrics to stdout. */
    public void printSummary() {
        List<MetricsSnapshot> snaps = getSnapshots();
        if (snaps.isEmpty()) {
            System.out.println("[" + label + "] No metrics collected.");
            return;
        }

        double avgCpu = 0, maxCpu = 0;
        double avgSysCpu = 0, maxSysCpu = 0;
        long maxHeapUsed = 0;

        for (MetricsSnapshot s : snaps) {
            avgCpu += s.processCpuLoad;
            if (s.processCpuLoad > maxCpu) maxCpu = s.processCpuLoad;
            avgSysCpu += s.systemCpuLoad;
            if (s.systemCpuLoad > maxSysCpu) maxSysCpu = s.systemCpuLoad;
            if (s.heapUsedBytes > maxHeapUsed) maxHeapUsed = s.heapUsedBytes;
        }
        avgCpu /= snaps.size();
        avgSysCpu /= snaps.size();

        // IO delta between first and last sample
        MetricsSnapshot first = snaps.get(0);
        MetricsSnapshot last = snaps.get(snaps.size() - 1);
        long ioReadDelta = last.ioReadBytes - first.ioReadBytes;
        long ioWriteDelta = last.ioWriteBytes - first.ioWriteBytes;

        System.out.println();
        System.out.printf("=== Metrics Summary [%s] (%d samples) ===%n", label, snaps.size());
        System.out.printf("  Process CPU  : avg=%.2f%%  max=%.2f%%%n", avgCpu * 100, maxCpu * 100);
        System.out.printf("  System CPU   : avg=%.2f%%  max=%.2f%%%n", avgSysCpu * 100, maxSysCpu * 100);
        System.out.printf("  Heap Memory  : max used=%.2f MB%n", maxHeapUsed / (1024.0 * 1024.0));
        System.out.printf("  Disk IO Read : %.2f MB%n", ioReadDelta / (1024.0 * 1024.0));
        System.out.printf("  Disk IO Write: %.2f MB%n", ioWriteDelta / (1024.0 * 1024.0));
        System.out.println();
    }
}
