# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Build (compile only, no database needed)
mvn compile

# Run unit tests (do not require a running database)
mvn test

# Run a single test class
mvn test -Dtest=DMDatabaseTest

# Run a single test method
mvn test -Dtest=DMDatabaseTest#testEscapeStringForDatabase

# Package into JAR
mvn package
```

Some tests (e.g. `DMLockServiceTest.testAcquireLock`, `DMLiquibaseIntegrationTest`) require a running DM database on `localhost:5236` with schema `LIQUIBASE_TEST`. These will fail silently or be skipped if the DB is unavailable.

## Architecture

This is a **Liquibase extension plugin** that adds support for Chinese domestic databases to Liquibase 4.20.0. It uses Liquibase's Java SPI mechanism — all extensions are registered via `META-INF/services/` files and auto-discovered at runtime.

### Supported Databases

- **DM (达梦)** — JDBC prefix `jdbc:dm://`, driver `dm.jdbc.driver.DmDriver`, port 5236. Oracle-compatible SQL dialect. Priority `PRIORITY_DEFAULT + 1` (higher than Oracle so DM connections are correctly identified even in Oracle-compatible mode).
- **KingBaseES (人大金仓)** — JDBC prefix `jdbc:kingbase8:`, driver `com.kingbase8.Driver`, port 54321. PostgreSQL-based SQL dialect.

### Extension Points by Layer

The plugin follows Liquibase's layered architecture. Each layer has a corresponding `META-INF/services/` registration:

| Layer | SPI Interface | Implementations |
|---|---|---|
| Database | `liquibase.database.Database` | `DMDatabase`, `KingBaseDatabase` |
| Lock Service | `liquibase.lockservice.LockService` | `DMLockService` (SELECT FOR UPDATE with manual txn control) |
| SQL Generator | `liquibase.sqlgenerator.SqlGenerator` | 6 generators for DM: `AddDefaultValue`, `AlterProcedure`, `CreateTrigger`, `DropTrigger`, `GetViewDefinition`, `InsertOrUpdate` |
| Data Type | `liquibase.datatype.LiquibaseDataType` | `DMBooleanType` (→NUMBER(1)), `DMTinyIntType` (→NUMBER(3)) |
| Change | `liquibase.change.Change` | `AlterProcedureChange`, `CreateTriggerChange`, `DropTriggerChange` |
| Structure | `liquibase.structure.DatabaseObject` | `Trigger` |
| Snapshot | `liquibase.snapshot.SnapshotGenerator` | `ProcedureSnapshotGenerator`, `TriggerSnapshotGenerator` |
| Change Generator | `liquibase.diff.output.changelog.ChangeGenerator` | 5 generators for procedure/trigger diff detection |

### Key Design Decisions

- **DMDatabase** extends `AbstractJdbcDatabase` and overrides schema name resolution (`getJdbcSchemaName`) to bypass Liquibase's underscore escaping, which breaks DM's literal `\` handling.
- **DMLockService** uses raw JDBC `Connection` with manual `setAutoCommit(false)` instead of Liquibase's executor, because DM requires an explicit transaction for `SELECT ... FOR UPDATE`.
- **DMDatabase.isCorrectDatabaseImplementation** matches both `"DM DBMS"` and `"oracle"` product names, since DM can run in Oracle compatibility mode.
- All classes live in the `liquibase.*` package namespace to match Liquibase's internal package structure — this is standard practice for Liquibase extensions.
