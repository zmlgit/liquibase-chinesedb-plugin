package liquibase.datatype.core;

import liquibase.database.Database;
import liquibase.database.core.DMDatabase;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;

@DataTypeInfo(name = "boolean", aliases = {"java.sql.Types.BOOLEAN", "java.lang.Boolean", "bit", "bool"}, minParameters = 0, maxParameters = 0, priority = LiquibaseDataType.PRIORITY_DEFAULT + 1)
public class DMBooleanType extends BooleanType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {

        if (database instanceof DMDatabase) {
            return new DatabaseDataType("NUMBER", 1);
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    @Override
    protected boolean isNumericBoolean(Database database) {

        if (database instanceof DMDatabase) {
            return true;
        }

        return super.isNumericBoolean(database);
    }

    @Override
    public String objectToSql(Object value, Database database) {
        return super.objectToSql(value, database);
    }
}
