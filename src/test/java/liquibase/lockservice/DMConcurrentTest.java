package liquibase.lockservice;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.core.DMDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class DMConcurrentTest {

    @Test
    void testConcurrentInit() throws Exception {
        int threadCount = 5;
        // Use a pool
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger createdCount = new AtomicInteger(0); // Track how many actually created the table
        
        // Clean up first
        cleanupDatabase();

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await(); // Wait for signal
                    
                    // Run logic
                    boolean created = runLockAndCheck();
                    if (created) {
                        createdCount.incrementAndGet();
                    }
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        endLatch.await();

        // 1. All threads should succeed without error (Locking works)
        Assertions.assertEquals(threadCount, successCount.get(), "All threads should succeed sequentially");
        Assertions.assertEquals(0, errorCount.get(), "There should be no errors");
        
        // 2. Only ONE thread should have performed the creation (Atomicity/Isolation verified)
        Assertions.assertEquals(1, createdCount.get(), "Only one thread should create the table, others see it exists");
    }

    /**
     * Simulates Liquibase logic: Acquire Lock -> Check Existence -> Create -> Release Lock.
     * Returns true if this thread created the table.
     */
    private boolean runLockAndCheck() throws Exception {
        String url = "jdbc:dm://localhost:5236?SCHEMA=LIQUIBASE_TEST";
        String username = "SYSDBA";
        String password = "SYSdba_001";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            // Ensure DMLockService is used
            liquibase.lockservice.LockService lockService = liquibase.lockservice.LockServiceFactory.getInstance().getLockService(database);
            lockService.init(); // Initialize checks for lock table existence
            
            // Wait for lock (StandardLockService checks periodically, DMLockService uses FOR UPDATE which blocks DB side)
            // But waitForLock calls acquireLock.
            lockService.waitForLock();
            
            boolean created = false;
            try {
                // Critical Section
                try (java.sql.Statement stmt = connection.createStatement()) {
                    // Check if table exists
                    try {
                        stmt.execute("SELECT 1 FROM \"LIQUIBASE_TEST\".\"T_CONCURRENT_TEST\"");
                        // Exists
                    } catch (Exception e) {
                        // Does not exist (Assume), Create it
                        stmt.execute("CREATE TABLE \"LIQUIBASE_TEST\".\"T_CONCURRENT_TEST\" (ID INT)");
                        created = true;
                        // Simulate work duration
                        Thread.sleep(200);
                    }
                }
            } finally {
                try {
                    lockService.releaseLock();
                } catch (Exception e) {
                    // ignore
                }
            }
            return created;
        }
    }
    
    private void cleanupDatabase() throws Exception {
        String url = "jdbc:dm://localhost:5236?SCHEMA=LIQUIBASE_TEST";
        String username = "SYSDBA";
        String password = "SYSdba_001";
        try (Connection connection = DriverManager.getConnection(url, username, password);
             java.sql.Statement stmt = connection.createStatement()) {
            
            // Ensure lock table is clean (unlock if stuck)
            try { stmt.execute("UPDATE \"LIQUIBASE_TEST\".\"DATABASECHANGELOGLOCK\" SET LOCKED=0"); } catch(Exception e) {}
            
            // Drop test table
            try { stmt.execute("DROP TABLE \"LIQUIBASE_TEST\".\"T_CONCURRENT_TEST\""); } catch(Exception e) {}
        }
    }
}
