package com.copybenchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Copies a Lucene index directory to a destination using the OS-level
 * {@code rsync} command.
 *
 * <p>The rsync command is invoked with {@code -a --delete} to produce a
 * faithful mirror of the source.  Because Lucene segments are immutable
 * once written, rsync's delta-transfer algorithm is efficient for
 * incremental syncs.</p>
 */
public class RsyncCopier {

    private final File sourceDir;
    private final File destDir;

    public RsyncCopier(File sourceDir, File destDir) {
        this.sourceDir = sourceDir;
        this.destDir = destDir;
    }

    /**
     * Perform the rsync copy.
     *
     * @return elapsed wall-clock time in milliseconds
     * @throws IOException if rsync fails or is not found
     */
    public long copy() throws IOException {
        // Ensure destination exists
        if (!destDir.exists() && !destDir.mkdirs()) {
            throw new IOException("Failed to create destination directory: " + destDir);
        }

        // Build rsync command.
        // Trailing '/' on source ensures contents are synced (not wrapped in an
        // extra directory level).
        String[] command = {
                "rsync",
                "-a",            // archive mode (recursive, preserve perms/times/symlinks)
                "--delete",      // remove files in dest that no longer exist in source
                sourceDir.getAbsolutePath() + "/",
                destDir.getAbsolutePath() + "/"
        };

        long startNs = System.nanoTime();

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO();  // stream rsync output to this process's stdout/stderr
        Process process = pb.start();

        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("rsync interrupted", e);
        }

        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;

        if (exitCode != 0) {
            throw new IOException("rsync exited with code " + exitCode);
        }

        return elapsedMs;
    }

    /** Recursively delete the destination directory. */
    public void cleanDestination() throws IOException {
        if (!destDir.exists()) return;
        Files.walkFileTree(destDir.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
