package liquibase.database.core;

import liquibase.Liquibase;
import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.BeforeEach;

class DMDatabaseTest {

    Liquibase liquibase;
    DMDatabase dmDatabase;
    @BeforeEach
    void setUp() {
      dmDatabase=new DMDatabase();
    }


}