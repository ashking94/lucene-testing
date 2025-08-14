package org.example;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final String TEST_FILE_NAME = "_1q_Lucene101_0.doc";
    private static final String TEST_FILE_SIZE = "100G";

    // OS Detection
    private static final String OS_NAME = System.getProperty("os.name").toLowerCase();
    private static final boolean IS_LINUX = OS_NAME.contains("linux");
    private static final boolean IS_MAC = OS_NAME.contains("mac") || OS_NAME.contains("darwin");
    private static final boolean IS_WINDOWS = OS_NAME.contains("windows");

    public static void main(final String... args) throws InterruptedException {
        // Get PID in main method
        pid = ProcessHandle.current().pid();
        System.out.println("Main thread PID: " + pid);
        System.out.println("Operating System: " + OS_NAME);

        // Get current working directory
        String currentDir = System.getProperty("user.dir");
        System.out.println("Working directory: " + currentDir);
        System.out.println("Test file: " + TEST_FILE_NAME + " (" + TEST_FILE_SIZE + ")");

        // Create test file if it doesn't exist
        try {
            createTestFileIfNeeded(currentDir);
        } catch (Exception e) {
            System.err.println("Failed to create test file: " + e.getMessage());
            e.printStackTrace();
            return;
        }

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
        System.out.println("Press Ctrl+C to exit or take heap dump now with: jcmd " + pid + " GC.heap_dump" + System.currentTimeMillis() + ".hprof");

        // Sleep with periodic GC and logging
        for (int i = 0; i < 300; i++) { // 5 minutes total
            Thread.sleep(1000); // Sleep for 1 second

            // Perform GC every 30 seconds
            if (i % 30 == 0 && i > 0) {
                System.out.println("\n=== Periodic GC cycle " + (i / 30) + " ===");
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
                System.out.println("Main thread still alive after " + (i / 60) + " minute(s), memory used: " + getUsedMemory() + " MB");
            }
        }
    }

    private static void createTestFileIfNeeded(String directory) throws IOException, InterruptedException {
        File testFile = new File(directory, TEST_FILE_NAME);

        if (testFile.exists()) {
            long fileSizeGB = testFile.length() / (1024L * 1024L * 1024L);
            System.out.println("Test file already exists: " + testFile.getAbsolutePath());
            System.out.println("File size: ~" + fileSizeGB + " GB (" + testFile.length() + " bytes)");
            return;
        }

        System.out.println("Creating test file: " + testFile.getAbsolutePath());
        System.out.println("Size: " + TEST_FILE_SIZE + " (this may take a moment...)");

        boolean success = false;

        if (IS_LINUX) {
            success = createFileLinux(testFile);
        } else if (IS_MAC) {
            success = createFileMac(testFile);
        } else if (IS_WINDOWS) {
            success = createFileWindows(testFile);
        } else {
            System.err.println("Unsupported operating system: " + OS_NAME);
            System.err.println("Attempting to use dd command as fallback...");
            success = createFileWithDD(testFile);
        }

        if (!success) {
            throw new IOException("Failed to create test file using all available methods");
        }

        // Verify file creation
        if (testFile.exists()) {
            long fileSizeGB = testFile.length() / (1024L * 1024L * 1024L);
            System.out.println("Test file created successfully!");
            System.out.println("File size: ~" + fileSizeGB + " GB (" + testFile.length() + " bytes)");
        } else {
            throw new IOException("Test file was not created successfully");
        }
    }

    private static boolean createFileLinux(File testFile) {
        try {
            System.out.println("Using Linux fallocate command...");
            ProcessBuilder pb = new ProcessBuilder("fallocate", "-l", TEST_FILE_SIZE, testFile.getAbsolutePath());
            return executeCommand(pb, "FALLOCATE");
        } catch (Exception e) {
            System.err.println("Linux fallocate failed: " + e.getMessage());
            System.err.println("Trying dd command as fallback...");
            return createFileWithDD(testFile);
        }
    }

    private static boolean createFileMac(File testFile) {
        try {
            System.out.println("Using macOS mkfile command...");
            // mkfile uses lowercase 'g' for gigabytes
            ProcessBuilder pb = new ProcessBuilder("mkfile", "-n", "100g", testFile.getAbsolutePath());
            return executeCommand(pb, "MKFILE");
        } catch (Exception e) {
            System.err.println("macOS mkfile failed: " + e.getMessage());
            System.err.println("Trying dd command as fallback...");
            return createFileWithDD(testFile);
        }
    }

    private static boolean createFileWindows(File testFile) {
        try {
            System.out.println("Using Windows fsutil command...");
            // fsutil requires size in bytes (100GB = 107374182400 bytes)
            ProcessBuilder pb = new ProcessBuilder("fsutil", "file", "createnew",
                    testFile.getAbsolutePath(), "107374182400");
            return executeCommand(pb, "FSUTIL");
        } catch (Exception e) {
            System.err.println("Windows fsutil failed: " + e.getMessage());
            System.err.println("Trying dd command as fallback...");
            return createFileWithDD(testFile);
        }
    }

    private static boolean createFileWithDD(File testFile) {
        try {
            System.out.println("Using dd command (universal fallback)...");
            // Create 100GB file with dd (100 * 1024 = 102400 blocks of 1MB each)
            ProcessBuilder pb = new ProcessBuilder("dd", "if=/dev/zero",
                    "of=" + testFile.getAbsolutePath(),
                    "bs=1M", "count=102400");
            return executeCommand(pb, "DD");
        } catch (Exception e) {
            System.err.println("DD command failed: " + e.getMessage());
            return false;
        }
    }

    private static boolean executeCommand(ProcessBuilder pb, String commandName) throws IOException, InterruptedException {
        Process process = pb.start();

        // Capture output
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
             BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(commandName + ": " + line);
            }

            while ((line = errorReader.readLine()) != null) {
                System.err.println(commandName + " ERROR: " + line);
            }
        }

        int exitCode = process.waitFor();

        if (exitCode == 0) {
            System.out.println(commandName + " completed successfully!");
            return true;
        } else {
            System.err.println(commandName + " failed with exit code: " + exitCode);
            return false;
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
            System.out.println("GC cycle " + (i + 1) + "/5 - Memory before: " + beforeGC + " MB");

            System.gc();

            try {
                Thread.sleep(1000); // Give GC time to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            long afterGC = getUsedMemory();
            System.out.println("GC cycle " + (i + 1) + "/5 - Memory after: " + afterGC + " MB, freed: " + (beforeGC - afterGC) + " MB");
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

        // Use current working directory and the test file
        String currentDir = System.getProperty("user.dir");
        MMapDirectory dir = new MMapDirectory(Paths.get(currentDir));

        System.out.println("\n=== Starting IndexInput creation test with file: " + TEST_FILE_NAME + " ===");
        System.out.println("Directory: " + currentDir);
        System.out.println("File size: " + TEST_FILE_SIZE);
        System.out.println("Creating IndexInputs in executor threads WITHOUT closing them");
        System.out.println("NOT holding references - allowing GC to potentially clean them up");

        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < 3000; i++) {
            final int iteration = i;

            // Submit task to create IndexInput but NOT close it and NOT hold reference
            Future<Void> future = executor.submit(() -> {
                try {
                    // Create IndexInput in executor thread
                    IndexInput indexInput = dir.openInput(TEST_FILE_NAME, IOContext.DEFAULT);

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

                    if (iteration % 100 == 0) {
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
            if (i % 100 == 0) {
                printVmmap(pid, "ITERATION " + i + " - Created " + createdCount.get() + " IndexInputs (no references held)");
                printMemoryInfo("ITERATION " + i + " MEMORY STATE", createdCount);
            }

            // Add a small delay every 100 iterations
            if (i % 100 == 0 && i > 0) {
                Thread.sleep(1000);
            }
        }

        System.out.println("\n=== Waiting for all IndexInput creation tasks to complete ===");

        // Map to track unique exceptions
        Map<String, ExceptionInfo> exceptionMap = new ConcurrentHashMap<>();
        int totalExceptions = 0;

        System.out.println("\n=== Waiting for all IndexInput creation tasks to complete ===");

        // Wait for all tasks to complete
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                totalExceptions++;
                String exceptionKey = e.getClass().getSimpleName() + ":" + (e.getMessage() != null ? e.getMessage() : "null");

                exceptionMap.computeIfAbsent(exceptionKey, k -> new ExceptionInfo(e))
                        .incrementCount();

                // Only print immediate error for first few exceptions or every 1000th exception
                if (totalExceptions % 100 == 0) {
                    System.err.println("Exception #" + totalExceptions + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                }
            }
        }

        // Print exception summary
        printExceptionSummary(exceptionMap, totalExceptions);

        // Capture detailed system state to file
        captureSystemStateToFile("AFTER_ALL_INDEXINPUT_CREATION", createdCount);

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

    private static void printExceptionSummary(Map<String, ExceptionInfo> exceptionMap, int totalExceptions) {
        if (totalExceptions == 0) {
            System.out.println("All IndexInput creation tasks completed successfully!");
            return;
        }

        System.out.println("\n" + "=".repeat(70));
        System.out.println("EXCEPTION SUMMARY");
        System.out.println("=".repeat(70));
        System.out.println("Total exceptions encountered: " + totalExceptions);
        System.out.println("Unique exception types: " + exceptionMap.size());
        System.out.println();

        // Sort exceptions by count (most frequent first)
        exceptionMap.entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e2.getValue().getCount(), e1.getValue().getCount()))
                .forEach(entry -> {
                    ExceptionInfo info = entry.getValue();
                    System.out.println("Exception Type: " + entry.getKey());
                    System.out.println("Count: " + info.getCount() + " occurrences");
                    System.out.println("Message: " + (info.getMessage() != null ? info.getMessage() : "No message"));

                    // Only print stack trace for exceptions that occurred less frequently
                    // or if there are very few unique exception types
                    if (info.getCount() <= 10 || exceptionMap.size() <= 3) {
                        System.out.println("Stack Trace:");
                        System.out.println(info.getStackTrace());
                    } else {
                        System.out.println("Stack Trace: [Suppressed - occurred " + info.getCount() + " times]");
                    }
                    System.out.println("-".repeat(50));
                });

        System.out.println("=".repeat(70));
    }

    private static void printMemoryInfo(String label, AtomicInteger createdCount) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        System.out.println("\n" + "-".repeat(60));
        System.out.println("MEMORY INFO - " + label);
        System.out.println("Thread: " + Thread.currentThread().getName());
        System.out.println("-".repeat(60));

        // Java Heap Memory Info
        System.out.println("=== Java Heap Memory ===");
        System.out.println("Used Memory: " + (usedMemory / 1024 / 1024) + " MB");
        System.out.println("Free Memory: " + (freeMemory / 1024 / 1024) + " MB");
        System.out.println("Total Memory: " + (totalMemory / 1024 / 1024) + " MB");
        System.out.println("Max Memory: " + (maxMemory / 1024 / 1024) + " MB");
        System.out.println("IndexInputs created: " + createdCount.get());

        // Add Linux-specific information
        if (IS_LINUX) {
            printLinuxSystemInfo();
            printLinuxProcessLimits();
            printLinuxFileDescriptorInfo();
        } else if (IS_MAC) {
            printMacSystemInfo();
        } else {
            System.out.println("=== OS-Specific Info ===");
            System.out.println("Detailed system info not available for: " + OS_NAME);
        }

        System.out.println("-".repeat(60));
    }

    private static void printLinuxSystemInfo() {
        System.out.println("=== Linux Process Memory Info ===");

        try {
            // Read /proc/<pid>/status for VmPeak, VmSize, VmRSS, etc.
            String statusFile = "/proc/" + pid + "/status";
            ProcessBuilder pb = new ProcessBuilder("grep", "-E",
                    "VmPeak|VmSize|VmRSS|VmHWM|VmData|VmStk|VmExe|VmLib|Threads", statusFile);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Format the output nicely
                    if (line.contains("VmPeak")) {
                        System.out.println("Peak Virtual Memory: " + formatLinuxMemory(line));
                    } else if (line.contains("VmSize")) {
                        System.out.println("Current Virtual Memory: " + formatLinuxMemory(line));
                    } else if (line.contains("VmRSS")) {
                        System.out.println("Resident Set Size: " + formatLinuxMemory(line));
                    } else if (line.contains("VmHWM")) {
                        System.out.println("Peak Resident Memory: " + formatLinuxMemory(line));
                    } else if (line.contains("VmData")) {
                        System.out.println("Data Segment Size: " + formatLinuxMemory(line));
                    } else if (line.contains("VmStk")) {
                        System.out.println("Stack Size: " + formatLinuxMemory(line));
                    } else if (line.contains("Threads")) {
                        System.out.println("Thread Count: " + line.split("\\s+")[1]);
                    }
                }
            }

            process.waitFor();

        } catch (Exception e) {
            System.err.println("Error reading Linux process status: " + e.getMessage());
        }

        // Read system-wide limits
        System.out.println("=== Linux System Limits ===");
        try {
            // Max memory mappings system-wide
            String maxMapCount = readLinuxSysctl("/proc/sys/vm/max_map_count");
            if (maxMapCount != null) {
                System.out.println("System Max Memory Maps: " + maxMapCount);
            }

            // Max file descriptors system-wide
            String maxFiles = readLinuxSysctl("/proc/sys/fs/file-max");
            if (maxFiles != null) {
                System.out.println("System Max File Descriptors: " + maxFiles);
            }

            // Current open files system-wide
            String openFiles = readLinuxSysctl("/proc/sys/fs/file-nr");
            if (openFiles != null) {
                String[] parts = openFiles.split("\\s+");
                if (parts.length >= 2) {
                    System.out.println("System Open Files: " + parts[0] + " (allocated: " + parts[1] + ")");
                }
            }

        } catch (Exception e) {
            System.err.println("Error reading Linux system limits: " + e.getMessage());
        }
    }

    private static void printLinuxProcessLimits() {
        System.out.println("=== Linux Process Limits ===");

        try {
            String limitsFile = "/proc/" + pid + "/limits";
            ProcessBuilder pb = new ProcessBuilder("grep", "-E",
                    "Max open files|Max address space|Max memory|Max processes", limitsFile);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Clean up the output format
                    String[] parts = line.split("\\s+", 4);
                    if (parts.length >= 4) {
                        String limitType = parts[0] + " " + parts[1] + " " + parts[2];
                        String softLimit = parts[3];
                        System.out.println(limitType + ": " + softLimit);
                    }
                }
            }

            process.waitFor();

        } catch (Exception e) {
            System.err.println("Error reading Linux process limits: " + e.getMessage());
        }
    }

    private static void printLinuxFileDescriptorInfo() {
        System.out.println("=== Linux File Descriptor Info ===");

        try {
            // Count current file descriptors
            String fdDir = "/proc/" + pid + "/fd";
            ProcessBuilder pb = new ProcessBuilder("ls", "-1", fdDir);
            Process process = pb.start();

            int fdCount = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                while (reader.readLine() != null) {
                    fdCount++;
                }
            }

            System.out.println("Current Open File Descriptors: " + fdCount);

            process.waitFor();

            // Count memory mappings
            String mapsFile = "/proc/" + pid + "/maps";
            pb = new ProcessBuilder("wc", "-l", mapsFile);
            process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = reader.readLine();
                if (line != null) {
                    String mappingCount = line.trim().split("\\s+")[0];
                    System.out.println("Total Memory Mappings: " + mappingCount);
                }
            }

            process.waitFor();

        } catch (Exception e) {
            System.err.println("Error reading Linux file descriptor info: " + e.getMessage());
        }
    }

    private static void printMacSystemInfo() {
        System.out.println("=== macOS System Info ===");

        try {
            // Get memory pressure information
            ProcessBuilder pb = new ProcessBuilder("memory_pressure");
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("pressure:")) {
                        System.out.println("Memory Pressure: " + line.trim());
                    }
                }
            }

            process.waitFor();

        } catch (Exception e) {
            // memory_pressure might not be available, try vm_stat
            try {
                ProcessBuilder pb2 = new ProcessBuilder("vm_stat");
                Process process2 = pb2.start();

                System.out.println("VM Statistics:");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process2.getInputStream()))) {
                    String line;
                    int lineCount = 0;
                    while ((line = reader.readLine()) != null && lineCount < 5) {
                        if (line.contains("Pages") && (line.contains("free") || line.contains("active") || line.contains("inactive"))) {
                            System.out.println("  " + line.trim());
                            lineCount++;
                        }
                    }
                }

                process2.waitFor();

            } catch (Exception e2) {
                System.err.println("Error reading macOS system info: " + e2.getMessage());
            }
        }

        // Get file descriptor info for macOS
        try {
            ProcessBuilder pb = new ProcessBuilder("lsof", "-p", String.valueOf(pid));
            Process process = pb.start();

            int fdCount = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                boolean isFirst = true;
                while ((line = reader.readLine()) != null) {
                    if (isFirst) {
                        isFirst = false; // Skip header
                        continue;
                    }
                    fdCount++;
                }
            }

            System.out.println("Open File Descriptors (lsof): " + fdCount);

            process.waitFor();

        } catch (Exception e) {
            System.err.println("Error reading macOS file descriptor info: " + e.getMessage());
        }
    }

    private static String readLinuxSysctl(String filePath) {
        try {
            ProcessBuilder pb = new ProcessBuilder("cat", filePath);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = reader.readLine();
                if (line != null) {
                    return line.trim();
                }
            }

            process.waitFor();

        } catch (Exception e) {
            // Silently fail for optional information
        }

        return null;
    }

    private static String formatLinuxMemory(String line) {
        try {
            String[] parts = line.split("\\s+");
            if (parts.length >= 3) {
                long valueKB = Long.parseLong(parts[1]);
                String unit = parts[2];

                if ("kB".equals(unit)) {
                    if (valueKB < 1024) {
                        return valueKB + " KB";
                    } else if (valueKB < 1024 * 1024) {
                        return String.format("%.2f MB", valueKB / 1024.0);
                    } else {
                        return String.format("%.2f GB", valueKB / (1024.0 * 1024.0));
                    }
                }
            }

            // Fallback to original format
            return line.substring(line.indexOf(':') + 1).trim();

        } catch (Exception e) {
            return line.substring(line.indexOf(':') + 1).trim();
        }
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
            ProcessBuilder pb;
            String commandDescription;

            if (IS_MAC) {
                pb = new ProcessBuilder("bash", "-c", "vmmap --wide " + pid + " | grep '_1q_Lucene101_0'");
                commandDescription = "vmmap (macOS)";
            } else if (IS_LINUX) {
                pb = new ProcessBuilder("bash", "-c", "cat /proc/" + pid + "/maps | grep '_1q_Lucene101_0'");
                commandDescription = "proc/maps (Linux)";
            } else {
                System.out.println("Memory mapping inspection not supported on " + OS_NAME);
                System.out.println("=".repeat(60));
                return;
            }

            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                int mappingCount = 0;
                long totalSizeKB = 0; // Track total virtual size if possible

                while ((line = reader.readLine()) != null) {
                    mappingCount++;

                    // Try to extract size information for additional context
                    if (IS_MAC && line.contains("[") && line.contains("]")) {
                        // macOS format: [...[ 48K    48K     0K     0K] ...]
                        try {
                            String sizeSection = line.substring(line.indexOf('[') + 1, line.indexOf(']'));
                            String[] parts = sizeSection.trim().split("\\s+");
                            if (parts.length > 0) {
                                String sizeStr = parts[0].trim();
                                if (sizeStr.endsWith("K")) {
                                    totalSizeKB += Long.parseLong(sizeStr.replace("K", ""));
                                } else if (sizeStr.endsWith("M")) {
                                    totalSizeKB += Long.parseLong(sizeStr.replace("M", "")) * 1024;
                                } else if (sizeStr.endsWith("G")) {
                                    totalSizeKB += Long.parseLong(sizeStr.replace("G", "")) * 1024 * 1024;
                                }
                            }
                        } catch (Exception e) {
                            // Ignore parsing errors, just count the mappings
                        }
                    }
                }

                if (mappingCount > 0) {
                    System.out.println("Found " + mappingCount + " memory mapping(s) for '_1q_Lucene101_0' file");
                    if (IS_MAC && totalSizeKB > 0) {
                        if (totalSizeKB < 1024) {
                            System.out.println("Total mapped size: ~" + totalSizeKB + " KB");
                        } else if (totalSizeKB < 1024 * 1024) {
                            System.out.println("Total mapped size: ~" + String.format("%.2f", totalSizeKB / 1024.0) + " MB");
                        } else {
                            System.out.println("Total mapped size: ~" + String.format("%.2f", totalSizeKB / (1024.0 * 1024.0)) + " GB");
                        }
                    }
                    System.out.println("Command used: " + commandDescription);
                } else {
                    System.out.println("No mappings found for '_1q_Lucene101_0' files");
                    System.out.println("Command used: " + commandDescription);
                }
            }

            // Check for errors
            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String errorLine;
                boolean hasErrors = false;
                while ((errorLine = errorReader.readLine()) != null) {
                    if (!hasErrors) {
                        System.out.println("Command errors:");
                        hasErrors = true;
                    }
                    System.err.println("  " + errorLine);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("Command failed with exit code: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Error executing memory mapping command: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("=".repeat(60));
    }

    private static void captureSystemStateToFile(String label, AtomicInteger createdCount) {
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String fileName = "system_state_" + label + "_" + timestamp + ".txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            writer.println("=".repeat(80));
            writer.println("SYSTEM STATE CAPTURE - " + label);
            writer.println("Timestamp: " + new Date());
            writer.println("PID: " + pid);
            writer.println("OS: " + OS_NAME);
            writer.println("=".repeat(80));
            writer.println();

            // 1. Memory Information
            captureMemoryInfoToFile(writer, "DETAILED MEMORY INFO", createdCount);

            // 2. Virtual Memory Mappings
            captureVmmapToFile(writer, "VIRTUAL MEMORY MAPPINGS");

            // 3. Process Memory Map (pmap -x for Linux, vmmap for Mac)
            captureProcessMemoryMapToFile(writer, "PROCESS MEMORY MAP");

            // 4. File Descriptor Information
            captureFileDescriptorInfoToFile(writer, "FILE DESCRIPTOR INFO");

            // 5. System Limits
            captureSystemLimitsToFile(writer, "SYSTEM LIMITS");

            writer.println("\n" + "=".repeat(80));
            writer.println("END OF SYSTEM STATE CAPTURE");
            writer.println("=".repeat(80));

            System.out.println("System state captured to file: " + fileName);

        } catch (IOException e) {
            System.err.println("Failed to write system state to file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void captureMemoryInfoToFile(PrintWriter writer, String label, AtomicInteger createdCount) {
        writer.println("=== " + label + " ===");

        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        writer.println("Thread: " + Thread.currentThread().getName());
        writer.println();

        // Java Heap Memory Info
        writer.println("=== Java Heap Memory ===");
        writer.println("Used Memory: " + (usedMemory / 1024 / 1024) + " MB");
        writer.println("Free Memory: " + (freeMemory / 1024 / 1024) + " MB");
        writer.println("Total Memory: " + (totalMemory / 1024 / 1024) + " MB");
        writer.println("Max Memory: " + (maxMemory / 1024 / 1024) + " MB");
        writer.println("IndexInputs created: " + createdCount.get());
        writer.println();

        // Add Linux-specific information
        if (IS_LINUX) {
            captureLinuxSystemInfoToFile(writer);
        } else if (IS_MAC) {
            captureMacSystemInfoToFile(writer);
        }

        writer.println();
    }

    private static void captureLinuxSystemInfoToFile(PrintWriter writer) {
        writer.println("=== Linux Process Memory Info ===");

        try {
            // Read /proc/<pid>/status for VmPeak, VmSize, VmRSS, etc.
            String statusFile = "/proc/" + pid + "/status";
            ProcessBuilder pb = new ProcessBuilder("grep", "-E",
                    "VmPeak|VmSize|VmRSS|VmHWM|VmData|VmStk|VmExe|VmLib|Threads", statusFile);
            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("VmPeak")) {
                        writer.println("Peak Virtual Memory: " + formatLinuxMemory(line));
                    } else if (line.contains("VmSize")) {
                        writer.println("Current Virtual Memory: " + formatLinuxMemory(line));
                    } else if (line.contains("VmRSS")) {
                        writer.println("Resident Set Size: " + formatLinuxMemory(line));
                    } else if (line.contains("VmHWM")) {
                        writer.println("Peak Resident Memory: " + formatLinuxMemory(line));
                    } else if (line.contains("VmData")) {
                        writer.println("Data Segment Size: " + formatLinuxMemory(line));
                    } else if (line.contains("VmStk")) {
                        writer.println("Stack Size: " + formatLinuxMemory(line));
                    } else if (line.contains("Threads")) {
                        writer.println("Thread Count: " + line.split("\\s+")[1]);
                    }
                }
            }

            process.waitFor();

        } catch (Exception e) {
            writer.println("Error reading Linux process status: " + e.getMessage());
        }

        writer.println();
        writer.println("=== Linux System Limits ===");
        try {
            String maxMapCount = readLinuxSysctl("/proc/sys/vm/max_map_count");
            if (maxMapCount != null) {
                writer.println("System Max Memory Maps: " + maxMapCount);
            }

            String maxFiles = readLinuxSysctl("/proc/sys/fs/file-max");
            if (maxFiles != null) {
                writer.println("System Max File Descriptors: " + maxFiles);
            }

            String openFiles = readLinuxSysctl("/proc/sys/fs/file-nr");
            if (openFiles != null) {
                String[] parts = openFiles.split("\\s+");
                if (parts.length >= 2) {
                    writer.println("System Open Files: " + parts[0] + " (allocated: " + parts[1] + ")");
                }
            }

        } catch (Exception e) {
            writer.println("Error reading Linux system limits: " + e.getMessage());
        }

        writer.println();
    }

    private static void captureMacSystemInfoToFile(PrintWriter writer) {
        writer.println("=== macOS System Info ===");

        try {
            ProcessBuilder pb = new ProcessBuilder("vm_stat");
            Process process = pb.start();

            writer.println("VM Statistics:");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.println("  " + line.trim());
                }
            }

            process.waitFor();

        } catch (Exception e) {
            writer.println("Error reading macOS system info: " + e.getMessage());
        }

        writer.println();
    }

    private static void captureVmmapToFile(PrintWriter writer, String label) {
        writer.println("=== " + label + " ===");

        try {
            ProcessBuilder pb;
            String commandDescription;

            if (IS_MAC) {
                pb = new ProcessBuilder("bash", "-c", "vmmap --wide " + pid + " | grep '_1q_Lucene101_0'");
                commandDescription = "vmmap (macOS)";
            } else if (IS_LINUX) {
                pb = new ProcessBuilder("bash", "-c", "cat /proc/" + pid + "/maps | grep '_1q_Lucene101_0'");
                commandDescription = "proc/maps (Linux)";
            } else {
                writer.println("Memory mapping inspection not supported on " + OS_NAME);
                return;
            }

            Process process = pb.start();
            int mappingCount = 0;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    mappingCount++;
                    writer.println(line);
                }
            }

            writer.println();
            writer.println("Total mappings found: " + mappingCount);
            writer.println("Command used: " + commandDescription);

            process.waitFor();

        } catch (Exception e) {
            writer.println("Error executing memory mapping command: " + e.getMessage());
        }

        writer.println();
    }

    private static void captureProcessMemoryMapToFile(PrintWriter writer, String label) {
        writer.println("=== " + label + " ===");

        try {
            ProcessBuilder pb;
            String commandDescription;

            if (IS_LINUX) {
                // Use pmap -x for detailed Linux memory map
                pb = new ProcessBuilder("pmap", "-x", String.valueOf(pid));
                commandDescription = "pmap -x (Linux)";
            } else if (IS_MAC) {
                // Use vmmap for detailed macOS memory map
                pb = new ProcessBuilder("vmmap", "--wide", String.valueOf(pid));
                commandDescription = "vmmap --wide (macOS)";
            } else {
                writer.println("Process memory map not supported on " + OS_NAME);
                return;
            }

            Process process = pb.start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.println(line);
                }
            }

            // Also capture any errors
            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String errorLine;
                boolean hasErrors = false;
                while ((errorLine = errorReader.readLine()) != null) {
                    if (!hasErrors) {
                        writer.println("Command errors:");
                        hasErrors = true;
                    }
                    writer.println("ERROR: " + errorLine);
                }
            }

            writer.println();
            writer.println("Command used: " + commandDescription);

            process.waitFor();

        } catch (Exception e) {
            writer.println("Error executing process memory map command: " + e.getMessage());
        }

        writer.println();
    }

    private static void captureFileDescriptorInfoToFile(PrintWriter writer, String label) {
        writer.println("=== " + label + " ===");

        if (IS_LINUX) {
            try {
                // Count current file descriptors
                String fdDir = "/proc/" + pid + "/fd";
                ProcessBuilder pb = new ProcessBuilder("ls", "-la", fdDir);
                Process process = pb.start();

                writer.println("File Descriptors (/proc/" + pid + "/fd):");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.println(line);
                    }
                }

                process.waitFor();

            } catch (Exception e) {
                writer.println("Error reading Linux file descriptor info: " + e.getMessage());
            }

        } else if (IS_MAC) {
            try {
                ProcessBuilder pb = new ProcessBuilder("lsof", "-p", String.valueOf(pid));
                Process process = pb.start();

                writer.println("Open Files (lsof -p " + pid + "):");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.println(line);
                    }
                }

                process.waitFor();

            } catch (Exception e) {
                writer.println("Error reading macOS file descriptor info: " + e.getMessage());
            }
        }

        writer.println();
    }

    private static void captureSystemLimitsToFile(PrintWriter writer, String label) {
        writer.println("=== " + label + " ===");

        if (IS_LINUX) {
            try {
                String limitsFile = "/proc/" + pid + "/limits";
                ProcessBuilder pb = new ProcessBuilder("cat", limitsFile);
                Process process = pb.start();

                writer.println("Process Limits (/proc/" + pid + "/limits):");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.println(line);
                    }
                }

                process.waitFor();

            } catch (Exception e) {
                writer.println("Error reading Linux process limits: " + e.getMessage());
            }
        } else if (IS_MAC) {
            try {
                ProcessBuilder pb = new ProcessBuilder("ulimit", "-a");
                Process process = pb.start();

                writer.println("Process Limits (ulimit -a):");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.println(line);
                    }
                }

                process.waitFor();

            } catch (Exception e) {
                writer.println("Error reading macOS process limits: " + e.getMessage());
            }
        }

        writer.println();
    }

}

class ExceptionInfo {
    private final String message;
    private final String stackTrace;
    private int count;

    public ExceptionInfo(Exception e) {
        this.message = e.getMessage();
        this.stackTrace = getStackTraceAsString(e);
        this.count = 1;
    }

    public void incrementCount() {
        this.count++;
    }

    public int getCount() {
        return count;
    }

    public String getMessage() {
        return message;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    private static String getStackTraceAsString(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
