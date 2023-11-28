import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

@Slf4j
public class H2toFile {

    private final Connection connection;
    private final String fileName = "result.txt";

    public H2toFile(Connection connection) {
        this.connection = connection;
    }

    public void execute() {
        Map<String, Set<String>> map = getTableNameAndPrimaryKeys();
        matchPrimaryKeysAndWrite(map);
    }

    public Map<String, Set<String>> getTableNameAndPrimaryKeys() {

        Map<String, Set<String>> map = new HashMap<>();

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM TABLE_LIST")) {

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String[] primaryKeyArray = resultSet.getString("PK").toLowerCase().replaceAll("\\s+", "").split(",");

                Set<String> set = new HashSet<>(Arrays.asList(primaryKeyArray));
                map.put(tableName, set);
            }

        } catch (SQLException e) {
            log.error("[getTableNameAndPrimaryKeys] database error occurred: ", e);
        }
        return map;
    }

    public void matchPrimaryKeysAndWrite(Map<String, Set<String>> map) {

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM TABLE_COLS");
             BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName, false))) {

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String columnName = resultSet.getString("COLUMN_NAME").toLowerCase();
                String columnType = resultSet.getString("COLUMN_TYPE");

                Set<String> set = map.get(tableName);
                if (set != null && set.contains(columnName)) {
                    String writeString = tableName + ", " + columnName + ", " + columnType + "\n";
                    bufferedWriter.write(writeString);
                }
            }
        } catch (SQLException e) {
            log.error("[matchPrimaryKeysAndWrite] database error occurred: ", e);
        } catch (IOException e) {
            log.error("[matchPrimaryKeyAndWrite] file write error: ", e);
        }
    }
}
