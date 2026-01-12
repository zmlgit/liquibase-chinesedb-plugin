package liquibase.lockservice;

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.core.DMDatabase;
import liquibase.database.jvm.JdbcConnection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public class DMLockServiceTest {

    @Test
    void testSupportAndPriority() {
        DMLockService service = new DMLockService();
        DMDatabase database = new DMDatabase();
        
        Assertions.assertTrue(service.supports(database));
        Assertions.assertEquals(liquibase.lockservice.LockService.PRIORITY_DATABASE, service.getPriority());
    }

    @Test
    void testAcquireLock() throws Exception {
        // Requires running DB. Maybe reuse testRealUrl setup?
        String url = "jdbc:dm://localhost:5236?SCHEMA=LIQUIBASE_TEST";
        String username = "SYSDBA";
        String password = "SYSdba_001";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            Assertions.assertTrue(database instanceof DMDatabase);
            
            LockService service = LockServiceFactory.getInstance().getLockService(database);
            Assertions.assertTrue(service instanceof DMLockService, "Should pick DMLockService");
            
            // Force reset
            service.reset();
            service.releaseLock(); // Clean up if locked
            
            boolean acquired = service.acquireLock();
            Assertions.assertTrue(acquired, "Should acquire lock successfully");
            
            // Verify lock table state?
            // service.listLocks();
            
            service.releaseLock();
        }
    }
}
