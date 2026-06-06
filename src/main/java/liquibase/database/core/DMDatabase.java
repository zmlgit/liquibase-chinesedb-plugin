package liquibase.database.core;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SequenceCurrentValueFunction;
import liquibase.statement.SequenceNextValueFunction;
import liquibase.statement.core.RawCallStatement;
import liquibase.structure.core.Schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class DMDatabase extends AbstractJdbcDatabase {


    private static final String PRODUCT_NAME = "DM DBMS";
    private static final String COMPATIBLE_MODE_ORACLE = "oracle";
    private static final Integer PORT = 5236;
    private final Set<String> reservedWords = new HashSet<>(); // 关键字集合
    private volatile String cachedSchemaName;

    /**
     * Default constructor for an object that represents the Oracle Database DBMS.
     */
    public DMDatabase() {
        super.unquotedObjectsAreUppercased = true;
        //noinspection HardCodedStringLiteral
        super.setCurrentDateTimeFunction("SYSDATE");
        // Setting list of Oracle's native functions
        //noinspection HardCodedStringLiteral
        dateFunctions.add(new DatabaseFunction("SYSDATE"));
        //noinspection HardCodedStringLiteral
        dateFunctions.add(new DatabaseFunction("SYSTIMESTAMP"));
        //noinspection HardCodedStringLiteral
        dateFunctions.add(new DatabaseFunction("CURRENT_TIMESTAMP"));
        //noinspection HardCodedStringLiteral
        super.sequenceNextValueFunction = "%s.nextval"; // 注意
        //noinspection HardCodedStringLiteral
        super.sequenceCurrentValueFunction = "%s.currval";
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        //noinspection HardCodedStringLiteral,HardCodedStringLiteral,HardCodedStringLiteral,HardCodedStringLiteral,
        // HardCodedStringLiteral
        //noinspection HardCodedStringLiteral,HardCodedStringLiteral,HardCodedStringLiteral,HardCodedStringLiteral,
        // HardCodedStringLiteral
        reservedWords.addAll(Arrays.asList("GROUP", "USER", "SESSION", "PASSWORD", "RESOURCE", "START", "SIZE", "UID", "DESC", "ORDER")); //more reserved words not returned by driver
        Connection sqlConn = null;
        if (!(conn instanceof OfflineConnection)) {
            try {
                /*
                 * Don't try to call getWrappedConnection if the conn instance is
                 * is not a JdbcConnection. This happens for OfflineConnection.
                 * see https://liquibase.jira.com/browse/CORE-2192
                 */
                if (conn instanceof JdbcConnection) {
                    sqlConn = ((JdbcConnection) conn).getWrappedConnection();
                }
            } catch (Exception e) {
                throw new UnexpectedLiquibaseException(e);
            }

            if (sqlConn != null) {
                try {
                    //noinspection HardCodedStringLiteral
                    reservedWords.addAll(Arrays.asList(sqlConn.getMetaData().getSQLKeywords().toUpperCase().split(",\\s*")));
                } catch (SQLException e) {
                    //noinspection HardCodedStringLiteral
                    Scope.getCurrentScope().getLog(getClass()).warning("Could get sql keywords on DM Database: " + e.getMessage());
                    //can not get keywords. Continue on
                }
            }
        }
        super.setConnection(conn);
    }

    @Override
    public String getJdbcCatalogName(CatalogAndSchema schema) {
        return null;
    }

    @Override
    public String getJdbcSchemaName(CatalogAndSchema schema) {
        String targetSchema = (schema.getSchemaName() == null) ? schema.getCatalogName() : schema.getSchemaName();
        // DM supports standard JDBC LIKE escape syntax (\_ matches literal underscore)
        // in DatabaseMetaData.getTables() and similar calls. Returning the correct schema
        // name ensures that DATABASECHANGELOG table lookups in Liquibase 4.25.0+ find the
        // table in the correct schema rather than scanning all schemas.
        //
        // Previously we returned null for schemas containing _ to work around a perceived
        // LIKE escaping issue, but that caused Liquibase to search ALL schemas, finding
        // DATABASECHANGELOG in the wrong schema and skipping table creation.
        return correctObjectName(targetSchema, Schema.class);
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends liquibase.structure.DatabaseObject> type) {
        return false;
    }

    @Override
    public String generatePrimaryKeyName(String tableName) {
        if (tableName.length() > 50) {
            //noinspection HardCodedStringLiteral
            return "PK_" + tableName.toUpperCase(Locale.US).substring(0, 27);
        } else {
            //noinspection HardCodedStringLiteral
            return "PK_" + tableName.toUpperCase(Locale.US);
        }
    }

    @Override
    public String getDateLiteral(String isoDate) {
        String normalLiteral = super.getDateLiteral(isoDate);

        if (isDateOnly(isoDate)) {
            //noinspection HardCodedStringLiteral
            return "TO_DATE(" + normalLiteral +
                    //noinspection HardCodedStringLiteral
                    ", 'YYYY-MM-DD')";
        } else if (isTimeOnly(isoDate)) {
            //noinspection HardCodedStringLiteral
            return "TO_DATE(" + normalLiteral +
                    //noinspection HardCodedStringLiteral
                    ", 'HH24:MI:SS')";
        } else if (isTimestamp(isoDate)) {
            //noinspection HardCodedStringLiteral
            return "TO_TIMESTAMP(" + normalLiteral +
                    //noinspection HardCodedStringLiteral
                    ", 'YYYY-MM-DD HH24:MI:SS.FF')";
        } else if (isDateTime(isoDate)) {
            int sepPos = normalLiteral.lastIndexOf('.');
            if (sepPos != -1) {
                normalLiteral = normalLiteral.substring(0, sepPos) + "'";
            }
            //noinspection HardCodedStringLiteral
            return "TO_DATE(" + normalLiteral +
                    //noinspection HardCodedStringLiteral
                    ", 'YYYY-MM-DD HH24:MI:SS')";
        }
        //noinspection HardCodedStringLiteral
        return "UNSUPPORTED:" + isoDate;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return super.supportsAutoIncrement();
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return false;
    }

    @Override
    public int getDataTypeMaxParameters(String dataTypeName) {
        //noinspection HardCodedStringLiteral
        if ("BINARY_FLOAT".equalsIgnoreCase(dataTypeName)) {
            return 0;
        }
        //noinspection HardCodedStringLiteral
        if ("BINARY_DOUBLE".equalsIgnoreCase(dataTypeName)) {
            return 0;
        }
        return super.getDataTypeMaxParameters(dataTypeName);
    }


    @Override
    public boolean jdbcCallsCatalogsSchemas() {
        return false;
    }

    @Override
    public String generateDatabaseFunctionValue(DatabaseFunction databaseFunction) {
        //noinspection HardCodedStringLiteral
        if ((databaseFunction != null) && "current_timestamp".equalsIgnoreCase(databaseFunction.toString())) {
            return databaseFunction.toString();
        }
        if ((databaseFunction instanceof SequenceNextValueFunction) || (databaseFunction instanceof SequenceCurrentValueFunction)) {
            String quotedSeq = super.generateDatabaseFunctionValue(databaseFunction);
            // replace "myschema.my_seq".nextval with "myschema"."my_seq".nextval
            return quotedSeq.replaceFirst("\"([^.\"]+)\\.([^.\"]+)\"", "\"$1\".\"$2\"");

        }

        return super.generateDatabaseFunctionValue(databaseFunction);
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    protected String getConnectionCatalogName() throws DatabaseException {
        return null;
    }

    @Override
    protected String getConnectionSchemaName() {
        if (cachedSchemaName != null) {
            return cachedSchemaName;
        }
        if (getConnection() instanceof OfflineConnection) {
            return ((OfflineConnection) getConnection()).getSchema();
        }
        try {
            //noinspection HardCodedStringLiteral
            String schema = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this).queryForObject(new RawCallStatement("select sys_context( 'userenv', 'current_schema' ) from dual"), String.class);
            if (schema != null) {
                cachedSchemaName = schema;
            }
            return schema;
        } catch (Exception e) {
            //noinspection HardCodedStringLiteral
            Scope.getCurrentScope().getLog(getClass()).warning("Error getting default schema", e);
        }
        return null;
    }



    @Override
    public boolean isReservedWord(String objectName) {
        return reservedWords.contains(objectName.toUpperCase());
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equals(conn.getDatabaseProductName()) || COMPATIBLE_MODE_ORACLE.equalsIgnoreCase(conn.getDatabaseProductName());
    }


    @Override
    public String getDefaultDriver(String url) {
        //noinspection HardCodedStringLiteral
        if (url.startsWith("jdbc:dm")) {
            return "dm.jdbc.driver.DmDriver";
        }
        return null;
    }

    @Override
    public String getShortName() {
        return "dm";
    }

    @Override
    public Integer getDefaultPort() {
        return PORT;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return true;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    /**
     * DM's DDL implicitly commits the current transaction, making DDL
     * invisible to other connections until the DML in the same changeset commits.
     * Returning false forces Liquibase to use autoCommit=true, ensuring each
     * SQL statement (DDL and DML alike) is immediately visible to all connections.
     * This prevents the race condition where Module A's CREATE TABLE is visible
     * to Module B, but Module A's INSERT INTO DATABASECHANGELOG is not.
     */
    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    // 高于oracle
    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT + 1;
    }

    @Override
    public int getDatabaseMajorVersion() throws DatabaseException {
        try {
            return getConnection().getDatabaseMajorVersion();
        } catch (Exception e) {
            // Fallback for drivers that return invalid version strings
            return 8; // Default to DM8 likely
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws DatabaseException {
        try {
            return getConnection().getDatabaseMinorVersion();
        } catch (Exception e) {
            // Fallback for drivers that return invalid version strings
            return 0;
        }
    }

    @Override
    public boolean isSystemObject(liquibase.structure.DatabaseObject example) {
        // Workaround: Liquibase normalizes the default schema to null.
        // This causes issues when comparing against the target schema (e.g. XXL_JOB) which is explicit.
        // We explicitly restore the schema if it matches the connection's default schema.
        if (example instanceof liquibase.structure.core.Table && example.getSchema() == null) {
            String explicitSchema = getConnectionSchemaName();
            if (explicitSchema != null) {
                ((liquibase.structure.core.Table) example).setSchema(new Schema((liquibase.structure.core.Catalog) null, explicitSchema));
            }
        }

        if (example instanceof liquibase.structure.core.Table && getDatabaseChangeLogTableName().equalsIgnoreCase(example.getName())) {
            return false;
        }
        // Exclude Recycle Bin tables
        if (example.getName() != null && example.getName().startsWith("BIN$")) {
            return true;
        }
        // Exclude system schemas
        if (example.getSchema() != null) {
            String schemaName = example.getSchema().getName();
            if ("CTISYS".equalsIgnoreCase(schemaName) || "SYS".equalsIgnoreCase(schemaName)) {
                return true;
            }
        }
        return super.isSystemObject(example);
    }

    @Override
    public CatalogAndSchema.CatalogAndSchemaCase getSchemaAndCatalogCase() {
        return CatalogAndSchema.CatalogAndSchemaCase.UPPER_CASE;
    }

    private static final java.util.regex.Pattern CREATE_TABLE_PATTERN =
            java.util.regex.Pattern.compile("(?i)(CREATE\\s+TABLE\\s+)(?!\\s*IF\\s+NOT\\s+EXISTS)");

    /**
     * Intercept all SQL execution and add IF NOT EXISTS to CREATE TABLE statements.
     * This makes all DDL idempotent, preventing race conditions when multiple modules
     * start concurrently against the same DM schema with independent changelog tables.
     */
    @Override
    public void execute(liquibase.statement.SqlStatement[] statements, java.util.List<liquibase.sql.visitor.SqlVisitor> sqlVisitors) throws liquibase.exception.LiquibaseException {
        if (statements != null) {
            for (int i = 0; i < statements.length; i++) {
                liquibase.statement.SqlStatement stmt = statements[i];
                if (stmt instanceof liquibase.statement.core.RawSqlStatement) {
                    String sql = ((liquibase.statement.core.RawSqlStatement) stmt).getSql();
                    if (sql != null && CREATE_TABLE_PATTERN.matcher(sql).find()) {
                        String rewritten = CREATE_TABLE_PATTERN.matcher(sql).replaceAll("$1IF NOT EXISTS ");
                        try {
                            statements[i] = new liquibase.statement.core.RawSqlStatement(rewritten);
                        } catch (Exception e) {
                            throw new liquibase.exception.DatabaseException(e);
                        }
                    }
                }
            }
        }
        super.execute(statements, sqlVisitors);
    }
}
