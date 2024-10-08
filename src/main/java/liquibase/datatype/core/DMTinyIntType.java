package liquibase.datatype.core;

import liquibase.database.Database;
import liquibase.database.core.DMDatabase;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;

@DataTypeInfo(name = "tinyint", aliases = "java.sql.Types.TINYINT", minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DEFAULT+1)
public class DMTinyIntType extends TinyIntType {

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {

        if (database instanceof DMDatabase) {
            return new DatabaseDataType("NUMBER", 3);
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    @Override
    public String objectToSql(Object value, Database database) {
        return super.objectToSql(value, database);
    }
}
