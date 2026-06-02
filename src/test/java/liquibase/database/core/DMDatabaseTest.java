package liquibase.database.core;

import org.junit.jupiter.api.BeforeEach;

import liquibase.Liquibase;

class DMDatabaseTest {

    Liquibase liquibaseInstance;
    DMDatabase dmDatabase;
    @BeforeEach
    void setUp() {
      dmDatabase=new DMDatabase();
    }

    @org.junit.jupiter.api.Test
    void testEscapeStringForDatabase() {
        String schemaName = "TEST_SCHEMA";
        String escapedSchemaName = dmDatabase.escapeStringForDatabase(schemaName);
        org.junit.jupiter.api.Assertions.assertEquals("TEST_SCHEMA", escapedSchemaName);
    }

    @org.junit.jupiter.api.Test
    void testSupportsSchemas() {
        org.junit.jupiter.api.Assertions.assertTrue(dmDatabase.supportsSchemas());
    }

    @org.junit.jupiter.api.Test
    void testIsSystemObject() {
        liquibase.structure.core.Table binTable = new liquibase.structure.core.Table(null, null, "BIN$12345");
        org.junit.jupiter.api.Assertions.assertTrue(dmDatabase.isSystemObject(binTable));

        liquibase.structure.core.Table normalTable = new liquibase.structure.core.Table(null, null, "MY_TABLE");
        org.junit.jupiter.api.Assertions.assertFalse(dmDatabase.isSystemObject(normalTable));
        
        liquibase.structure.core.Schema sysSchema = new liquibase.structure.core.Schema((String)null, "CTISYS");
        liquibase.structure.core.Table sysTable = new liquibase.structure.core.Table((String)null, "CTISYS", "SYS_TABLE");
        org.junit.jupiter.api.Assertions.assertTrue(dmDatabase.isSystemObject(sysTable));
    }

    @org.junit.jupiter.api.Test
    void testSupportsCatalogInObjectName() {
        org.junit.jupiter.api.Assertions.assertFalse(dmDatabase.supportsCatalogInObjectName(liquibase.structure.core.Table.class));
    }

    @org.junit.jupiter.api.Test
    void testGetJdbcSchemaName() {
        liquibase.CatalogAndSchema catalogAndSchema = new liquibase.CatalogAndSchema("CATALOG", "SCHEMA");
        org.junit.jupiter.api.Assertions.assertEquals("SCHEMA", dmDatabase.getJdbcSchemaName(catalogAndSchema));

        liquibase.CatalogAndSchema catalogOnly = new liquibase.CatalogAndSchema("CATALOG", null);
        org.junit.jupiter.api.Assertions.assertEquals("CATALOG", dmDatabase.getJdbcSchemaName(catalogOnly));
    }

    @org.junit.jupiter.api.Test
    void testGetJdbcSchemaNameUnderscoreBypassesEscaping() {
        // Schema with underscore should return null to bypass Liquibase's LIKE escaping
        liquibase.CatalogAndSchema underscoreSchema = new liquibase.CatalogAndSchema(null, "XXL_JOB");
        org.junit.jupiter.api.Assertions.assertNull(dmDatabase.getJdbcSchemaName(underscoreSchema),
                "Schema with underscore should return null to bypass JDBC LIKE escaping");

        // Schema without underscore should return the schema name normally
        liquibase.CatalogAndSchema normalSchema = new liquibase.CatalogAndSchema(null, "MYSCHEMA");
        org.junit.jupiter.api.Assertions.assertEquals("MYSCHEMA", dmDatabase.getJdbcSchemaName(normalSchema));

        // Multiple underscores
        liquibase.CatalogAndSchema multiUnderscore = new liquibase.CatalogAndSchema(null, "MY_APP_SCHEMA");
        org.junit.jupiter.api.Assertions.assertNull(dmDatabase.getJdbcSchemaName(multiUnderscore));
    }

    @org.junit.jupiter.api.Test
    void testGetSchemaAndCatalogCase() {
        org.junit.jupiter.api.Assertions.assertEquals(liquibase.CatalogAndSchema.CatalogAndSchemaCase.UPPER_CASE, dmDatabase.getSchemaAndCatalogCase());
    }
}