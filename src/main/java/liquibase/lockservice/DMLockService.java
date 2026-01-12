package liquibase.lockservice;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.core.DMDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Date;

public class DMLockService extends StandardLockService {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE + 1;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof DMDatabase;
    }

    @Override
    public boolean acquireLock() throws LockException {
        if (hasChangeLogLock) {
            return true;
        }

        try {
            if (!isDatabaseChangeLogLockTableInitialized(hasDatabaseChangeLogLockTable)) {
                return false; 
            }
            
            // Dameng requires transaction for FOR UPDATE
            // We use raw JDBC connection to avoid Scope/Executor threading issues
            if (!(database.getConnection() instanceof JdbcConnection)) {
                 return super.acquireLock(); // Fallback if not JDBC
            }
            
            Connection conn = ((JdbcConnection) database.getConnection()).getWrappedConnection();
            boolean originalAutoCommit = conn.getAutoCommit();
            if (originalAutoCommit) {
                conn.setAutoCommit(false);
            }

            try {
                String lockTable = database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName());
                
                // 1. SELECT FOR UPDATE
                // Note: ID column type? Usually int.
                String lockQuery = "SELECT LOCKED FROM " + lockTable + " WHERE ID=1 FOR UPDATE";
                
                boolean locked = false;
                try (PreparedStatement stmt = conn.prepareStatement(lockQuery);
                     ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        locked = rs.getBoolean(1);
                    }
                }

                if (locked) {
                    // Lock is logically held by someone else
                    conn.rollback();
                    return false;
                }

                // 2. UPDATE
                database.setCanCacheLiquibaseTableInfo(true);
                
                String lockedBy = getHostname() + " (" + getHostAddress() + ")";
                
                // Use parameterized query
                String updateQuery = "UPDATE " + lockTable + " SET LOCKED = 1, LOCKEDBY = ?, LOCKGRANTED = ? WHERE ID=1";
                
                try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                    stmt.setString(1, lockedBy);
                    stmt.setTimestamp(2, new Timestamp(new Date().getTime()));
                    stmt.executeUpdate();
                }
                
                conn.commit();
                hasChangeLogLock = true;
                return true;

            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (Exception rollbackEx) {
                    // Ignore
                }
                throw e;
            } finally {
               if (originalAutoCommit) {
                   conn.setAutoCommit(true);
               }
            }

        } catch (Exception e) {
             throw new LockException(e);
        }
    }
    
    private String getHostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName(); 
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    private String getHostAddress() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
