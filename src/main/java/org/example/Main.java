package org.example;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    private static volatile boolean testCompleted = false;
    private static long pid;
    private static String directoryPath;

    public static void main(final String... args) throws InterruptedException {
        // Parse command line arguments
        parseArguments(args);

        // Get PID in main method
        pid = ProcessHandle.current().pid();
        System.out.println("Main thread PID: " + pid);
        System.out.println("Using directory path: " + directoryPath);

        // Keep main method minimal to avoid variables showing up in heap dump
        Thread testThread = new Thread(Main::runTest, "Main-Test-Thread");
        testThread.start();

        // Wait for test completion signal
        while (!testCompleted) {
            Thread.sleep(1000);
        }

        System.out.println("\n=== Test completed, starting heap dump analysis phase in main thread ===");
        System.out.println("Main thread: " + Thread.currentThread().getName());

        // Perform GC cycles with logging in main thread
        performGCWithLogging();

        // Keep the process alive for heap dump analysis
        System.out.println("Keeping process alive for heap dump analysis...");
        System.out.println("Press Ctrl+C to exit or take heap dump now with: jcmd " + pid + " Heap.dump heap_dump_" + System.currentTimeMillis() + ".hprof");

        // Sleep with periodic GC and logging
        for (int i = 0; i < 300; i++) { // 5 minutes total
            Thread.sleep(1000); // Sleep for 1 second

            // Perform GC every 30 seconds
            if (i % 30 == 0 && i > 0) {
                System.out.println("\n=== Periodic GC cycle " + (i/30) + " ===");
                long beforeGC = getUsedMemory();
                System.gc();
                Thread.sleep(500); // Give GC time
                long afterGC = getUsedMemory();
                System.out.println("Memory before GC: " + beforeGC + " MB");
                System.out.println("Memory after GC: " + afterGC + " MB");
                System.out.println("Memory freed: " + (beforeGC - afterGC) + " MB");
            }

            // Print status every minute
            if (i % 60 == 0 && i > 0) {
                System.out.println("Main thread still alive after " + (i/60) + " minute(s), memory used: " + getUsedMemory() + " MB");
            }
        }
    }

    private static void parseArguments(String[] args) {
        // Default directory path
        String defaultPath = "/Users/ssashish/dev/docker/opensearch/opensearch-1m2d-s3-data1/nodes/0/_state";

        if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            directoryPath = args[0].trim();
            System.out.println("Using provided directory path: " + directoryPath);
        } else {
            directoryPath = defaultPath;
            System.out.println("No directory path provided, using default: " + directoryPath);
        }

        // Validate that the path exists
        try {
            Path path = Paths.get(directoryPath);
            if (!path.toFile().exists()) {
                System.err.println("WARNING: Directory does not exist: " + directoryPath);
            } else if (!path.toFile().isDirectory()) {
                System.err.println("WARNING: Path is not a directory: " + directoryPath);
            } else {
                System.out.println("Directory validated successfully: " + directoryPath);
            }
        } catch (Exception e) {
            System.err.println("ERROR: Invalid directory path: " + directoryPath + " - " + e.getMessage());
        }

        // Print usage information
        if (args.length == 0) {
            System.out.println("\nUsage: java -jar target/lucene-test-1.0-SNAPSHOT.jar [directory_path]");
            System.out.println("  directory_path: Optional path to Lucene index directory");
            System.out.println("  If not provided, uses: " + defaultPath);
        }
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
    }

    private static void performGCWithLogging() {
        System.out.println("\n=== Performing aggressive GC with logging ===");

        for (int i = 0; i < 5; i++) {
            long beforeGC = getUsedMemory();
            System.out.println("GC cycle " + (i+1) + "/5 - Memory before: " + beforeGC + " MB");

            System.gc();

            try {
                Thread.sleep(1000); // Give GC time to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            long afterGC = getUsedMemory();
            System.out.println("GC cycle " + (i+1) + "/5 - Memory after: " + afterGC + " MB, freed: " + (beforeGC - afterGC) + " MB");
        }

        // Take heap dump after aggressive GC
        System.out.println("\n=== Taking heap dump after aggressive GC ===");
        takeHeapDump(pid);
    }

    private static void runTest() {
        try {
            executeTest();
        } catch (Exception e) {
            System.err.println("Error in test execution: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Signal test completion
            testCompleted = true;
            System.out.println("Test thread completed, signaling main thread...");
        }
    }

    private static void executeTest() throws IOException, InterruptedException {
        System.out.println("Running test in thread: " + Thread.currentThread().getName());

        // Create executor with multiple threads to simulate concurrent access
        ExecutorService executor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "IndexInput-Creator-Thread-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });

        // Counter to track how many IndexInputs were created
        AtomicInteger createdCount = new AtomicInteger(0);

        // Print initial memory map state
        printVmmap(pid, "INITIAL STATE - Before opening any files");
        printMemoryInfo("INITIAL MEMORY STATE", createdCount);

        // Use the directory path from command line argument (or default)
        String fileName = "_1q_Lucene101_0.doc";
        MMapDirectory dir = new MMapDirectory(Path.of(directoryPath));

        System.out.println("\n=== Starting IndexInput creation test with file: " + fileName + " ===");
        System.out.println("Directory: " + directoryPath);
        System.out.println("Creating IndexInputs in executor threads WITHOUT closing them");
        System.out.println("NOT holding references - allowing GC to potentially clean them up");

        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < 100000; i++) {
            final int iteration = i;

            // Submit task to create IndexInput but NOT close it and NOT hold reference
            Future<Void> future = executor.submit(() -> {
                try {
                    // Create IndexInput in executor thread
                    IndexInput indexInput = dir.openInput(fileName, IOContext.DEFAULT);

                    // Touch some data to ensure it's actually used
                    indexInput.seek(0);
                    if (indexInput.length() > 0) {
                        indexInput.readByte(); // Just read one byte
                    }

                    // Increment counter
                    int count = createdCount.incrementAndGet();

                    // DO NOT CLOSE - This is intentional
                    // indexInput.close(); // <-- Deliberately commented out

                    // DO NOT STORE REFERENCE - Let it go out of scope
                    // This allows the IndexInput object to become eligible for GC
                    // But the question is: will the underlying memory mapping remain?

                    if (iteration % 1000 == 0) {
                        System.out.println("Thread " + Thread.currentThread().getName() +
                                " created IndexInput #" + iteration +
                                " (Total created: " + count + ")");
                    }

                    return null;
                } catch (IOException e) {
                    throw new RuntimeException("Error creating IndexInput", e);
                }
            });

            futures.add(future);

            // Print vmmap at specific intervals
            if (i % 10000 == 0) {
                printVmmap(pid, "ITERATION " + i + " - Created " + createdCount.get() + " IndexInputs (no references held)");
                printMemoryInfo("ITERATION " + i + " MEMORY STATE", createdCount);
            }

            // Add a small delay every 100 iterations
            if (i % 100 == 0 && i > 0) {
                Thread.sleep(10);
            }
        }

        System.out.println("\n=== Waiting for all IndexInput creation tasks to complete ===");

        // Wait for all tasks to complete
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                System.err.println("Error in IndexInput creation task: " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("Total IndexInputs created: " + createdCount.get());

        printVmmap(pid, "AFTER creating all IndexInputs");
        printMemoryInfo("AFTER creating all IndexInputs", createdCount);

        // Force garbage collection to see what happens
        System.out.println("\n=== Forcing garbage collection in test thread ===");
        System.gc();
        Thread.sleep(1000); // Give GC time to run
        System.gc();
        Thread.sleep(1000);

        printVmmap(pid, "AFTER first garbage collection");
        printMemoryInfo("AFTER first garbage collection", createdCount);

        System.out.println("\n=== Cleaning up test thread resources ===");

        // Shutdown executor
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        printVmmap(pid, "AFTER shutting down executor");

        dir.close();
        printVmmap(pid, "AFTER closing directory");

        System.out.println("UNCLOSED INDEXINPUT TEST FINISHED in test thread");
        System.out.println("Total IndexInputs created: " + createdCount.get());
        printVmmap(pid, "FINAL STATE - Test thread finished");
        printMemoryInfo("FINAL MEMORY STATE - Test thread", createdCount);
    }

    private static void printMemoryInfo(String label, AtomicInteger createdCount) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        System.out.println("\n" + "-".repeat(40));
        System.out.println("MEMORY INFO - " + label);
        System.out.println("Thread: " + Thread.currentThread().getName());
        System.out.println("-".repeat(40));
        System.out.println("Used Memory: " + (usedMemory / 1024 / 1024) + " MB");
        System.out.println("Free Memory: " + (freeMemory / 1024 / 1024) + " MB");
        System.out.println("Total Memory: " + (totalMemory / 1024 / 1024) + " MB");
        System.out.println("Max Memory: " + (maxMemory / 1024 / 1024) + " MB");
        System.out.println("IndexInputs created: " + createdCount.get());
        System.out.println("-".repeat(40));
    }

    private static void takeHeapDump(long pid) {
        try {
            String fileName = "heap_dump_" + System.currentTimeMillis() + ".hprof";
            String filePath = System.getProperty("user.dir") + "/" + fileName;

            // Use jcmd to take heap dump
            ProcessBuilder pb = new ProcessBuilder("jcmd", String.valueOf(pid), "Heap.dump", filePath);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("HEAP DUMP: " + line);
                }
            }

            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String errorLine;
                while ((errorLine = errorReader.readLine()) != null) {
                    System.err.println("HEAP DUMP ERROR: " + errorLine);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Heap dump created successfully: " + filePath);
            } else {
                System.err.println("Heap dump failed with exit code: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Error taking heap dump: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printVmmap(long pid, String label) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("VMMAP OUTPUT - " + label);
        System.out.println("Thread: " + Thread.currentThread().getName());
        System.out.println("=".repeat(60));

        try {
            ProcessBuilder pb = new ProcessBuilder("bash", "-c",
                    "vmmap --wide " + pid + " | grep '_1q_Lucene101_0'");
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                boolean foundMappings = false;

                while ((line = reader.readLine()) != null) {
                    foundMappings = true;
                    System.out.println(line);
                }

                if (!foundMappings) {
                    System.out.println("No mappings found for '_1q_Lucene101_0' files");
                }
            }

            process.waitFor();

        } catch (Exception e) {
            System.err.println("Error executing vmmap command: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("=".repeat(60));
    }
}
