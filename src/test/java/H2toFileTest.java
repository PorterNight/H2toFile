import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class H2toFileTest {

    private static Connection connection;
    private static final String fileName = "result.txt";

    @BeforeAll
    static void setUp() throws SQLException {

        final String JDBC_URL = "jdbc:h2:mem:test";
        final String USER = "root";
        final String PASSWORD = "root";

        connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);

        try (Statement statement = connection.createStatement()) {
            String sqlCreate = "CREATE TABLE TABLE_LIST (TABLE_NAME VARCHAR(32), PK VARCHAR(256))";
            statement.execute(sqlCreate);
            sqlCreate = "CREATE TABLE TABLE_COLS (TABLE_NAME VARCHAR(32), COLUMN_NAME VARCHAR(32), COLUMN_TYPE VARCHAR(32))";
            statement.execute(sqlCreate);

            sqlCreate = "INSERT INTO TABLE_LIST VALUES ('Users', 'ID'), ('accounts', 'account, account_id'), ('orders', 'order_date')";
            statement.execute(sqlCreate);

            sqlCreate = "INSERT INTO TABLE_COLS VALUES ('Users', 'ID', 'INT'), ('accounts', 'Account', 'VARCHAR(32)'), " +
                    "('accounts', 'account_id', 'INT'), ('orders', 'order_date', 'TIMESTAMP')";
            statement.execute(sqlCreate);
        }
    }

    @AfterAll
    static void finish() throws SQLException {
        connection.close();
    }

    @Test
    void testExecuteMethod() throws IOException {

        H2toFile h2toFile = new H2toFile(connection);
        h2toFile.execute();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line = reader.readLine();
            assertEquals("Users, id, INT", line);

            line = reader.readLine();
            assertEquals("accounts, account, VARCHAR(32)", line);

            line = reader.readLine();
            assertEquals("accounts, account_id, INT", line);

            line = reader.readLine();
            assertEquals("orders, order_date, TIMESTAMP", line);
        }
    }
}
