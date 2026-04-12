package com.copybenchmark;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Copies a Lucene index directory to a destination by creating
 * <strong>hard links</strong> for every regular file.
 *
 * <p>Because Lucene segment files are write-once / immutable, hard links
 * are safe: the destination will share the same on-disk data blocks as the
 * source without any risk of corruption.  The link operation is O(number of
 * files), not O(total bytes), so it completes near-instantly even for very
 * large indices.</p>
 */
public class HardLinkCopier {

    private final File sourceDir;
    private final File destDir;

    public HardLinkCopier(File sourceDir, File destDir) {
        this.sourceDir = sourceDir;
        this.destDir = destDir;
    }

    /**
     * Create hard links for every file in the source directory tree.
     *
     * @return elapsed wall-clock time in milliseconds
     * @throws IOException on any filesystem error
     */
    public long copy() throws IOException {
        if (!destDir.exists() && !destDir.mkdirs()) {
            throw new IOException("Failed to create destination directory: " + destDir);
        }

        long startNs = System.nanoTime();

        final Path srcRoot = sourceDir.toPath();
        final Path dstRoot = destDir.toPath();

        Files.walkFileTree(srcRoot, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                Path target = dstRoot.resolve(srcRoot.relativize(dir));
                if (!Files.exists(target)) {
                    Files.createDirectories(target);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                Path target = dstRoot.resolve(srcRoot.relativize(file));
                Files.createLink(target, file);
                return FileVisitResult.CONTINUE;
            }
        });

        return (System.nanoTime() - startNs) / 1_000_000;
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
