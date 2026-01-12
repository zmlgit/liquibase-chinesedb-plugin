package liquibase.database.core;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public class DMLiquibaseIntegrationTest {

    @Test
    void testRealUrl() throws Exception {
        // Default DM credentials
        String url = "jdbc:dm://localhost:5236?SCHEMA=LIQUIBASE_TEST";
        String username = "SYSDBA";
        String password = "SYSdba_001";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            
            try (Liquibase liquibase = new Liquibase("db/changelog/dm.changelog-master.xml", new ClassLoaderResourceAccessor(), database)) {
                System.out.println("DEBUG: Starting Run 1");
                liquibase.update(new Contexts(), new LabelExpression());
                
                // Verify idempotency (Run 2)
                System.out.println("DEBUG: Starting Idempotency Run");
                liquibase.update(new Contexts(), new LabelExpression());
                
                System.out.println("Liquibase update completed successfully!");
            }
        }
    }


}
