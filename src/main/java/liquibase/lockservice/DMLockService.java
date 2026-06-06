package liquibase.lockservice;

import liquibase.database.Database;
import liquibase.database.core.DMDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LockException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;

/**
 * DM-specific lock service that uses SELECT FOR UPDATE for row-level locking.
 * <p>
 * Only overrides {@link #acquireLock()} and {@link #releaseLock()} for the DM-specific
 * lock pattern. All table creation/initialization is delegated to the parent
 * {@link StandardLockService}.
 * <p>
 * Cross-version compatible with Liquibase 4.9.3, 4.20.0, 4.25.0+.
 * Only uses public API from StandardLockService (no field access except
 * {@code hasChangeLogLock} which is protected in all versions).
 */
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

        // Ensure lock table exists before attempting to acquire lock.
        // Parent StandardLockService.acquireLock() calls init() first,
        // but since we override acquireLock(), we must call init() ourselves.
        try {
            init();
        } catch (Exception e) {
            throw new LockException(e);
        }

        // Use the public isDatabaseChangeLogLockTableInitialized(boolean) method
        // which exists in ALL versions: 4.9.3 (public), 4.20.0 (protected), 4.25.0 (public).
        if (!isDatabaseChangeLogLockTableInitialized(false)) {
            return false;
        }

        // DM-specific: use SELECT FOR UPDATE for row-level locking
        if (!(database.getConnection() instanceof JdbcConnection)) {
            return super.acquireLock();
        }

        try {
            JdbcConnection jdbcConn = (JdbcConnection) database.getConnection();
            Connection conn = jdbcConn.getWrappedConnection();
            boolean originalAutoCommit = conn.getAutoCommit();

            try {
                if (originalAutoCommit) {
                    conn.setAutoCommit(false);
                }

                String lockTable = database.escapeTableName(
                        database.getLiquibaseCatalogName(),
                        database.getLiquibaseSchemaName(),
                        database.getDatabaseChangeLogLockTableName());

                // 1. SELECT FOR UPDATE — acquire row-level lock
                String lockQuery = "SELECT LOCKED FROM " + lockTable + " WHERE ID=1 FOR UPDATE";

                boolean locked = false;
                try (PreparedStatement stmt = conn.prepareStatement(lockQuery);
                     ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        locked = rs.getBoolean(1);
                    }
                }

                if (locked) {
                    conn.rollback();
                    return false;
                }

                // 2. UPDATE — set locked flag
                database.setCanCacheLiquibaseTableInfo(true);
                String lockedBy = getHostname() + " (" + getHostAddress() + ")";

                String updateQuery = "UPDATE " + lockTable +
                        " SET LOCKED = 1, LOCKEDBY = ?, LOCKGRANTED = ? WHERE ID=1";

                try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                    stmt.setString(1, lockedBy);
                    stmt.setTimestamp(2, new Timestamp(new Date().getTime()));
                    stmt.executeUpdate();
                }

                conn.commit();
                hasChangeLogLock = true;
                return true;

            } catch (Exception e) {
                try { conn.rollback(); } catch (Exception ignored) {}
                throw new LockException(e);
            } finally {
                // Always restore autoCommit — critical for subsequent Liquibase operations
                if (originalAutoCommit) {
                    try { conn.setAutoCommit(true); } catch (Exception ignored) {}
                }
            }
        } catch (LockException e) {
            throw e;
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    @Override
    public void releaseLock() throws LockException {
        if (!hasChangeLogLock) {
            return;
        }

        if (!(database.getConnection() instanceof JdbcConnection)) {
            super.releaseLock();
            return;
        }

        try {
            JdbcConnection jdbcConn = (JdbcConnection) database.getConnection();
            Connection conn = jdbcConn.getWrappedConnection();
            boolean originalAutoCommit = conn.getAutoCommit();

            try {
                if (originalAutoCommit) {
                    conn.setAutoCommit(false);
                }

                String lockTable = database.escapeTableName(
                        database.getLiquibaseCatalogName(),
                        database.getLiquibaseSchemaName(),
                        database.getDatabaseChangeLogLockTableName());

                String updateQuery = "UPDATE " + lockTable +
                        " SET LOCKED = 0, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID=1";

                try (PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
                    stmt.executeUpdate();
                }

                conn.commit();
                hasChangeLogLock = false;
                database.setCanCacheLiquibaseTableInfo(false);
            } catch (Exception e) {
                try { conn.rollback(); } catch (Exception ignored) {}
                throw new LockException(e);
            } finally {
                if (originalAutoCommit) {
                    try { conn.setAutoCommit(true); } catch (Exception ignored) {}
                }
            }
        } catch (LockException e) {
            throw e;
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
