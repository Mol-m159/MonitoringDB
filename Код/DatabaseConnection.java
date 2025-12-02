package imitator;

import util.Config;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatabaseConnection {
    private static Connection connection;

    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            Config config = new Config();
            String url = config.getDbUrl();
            String username = config.getDbUsername();
            String password = config.getDbPassword();

            System.out.println("Попытка подключения к БД:");
            System.out.println("URL: " + url);
            System.out.println("Username: " + username);

            try {
                connection = DriverManager.getConnection(url, username, password);
                System.out.println("Подключение к БД успешно установлено!");

                // Проверим, что подключение работает
                var statement = connection.createStatement();
                var resultSet = statement.executeQuery("SELECT DB_NAME() as db_name");
                if (resultSet.next()) {
                    System.out.println("База данных: " + resultSet.getString("db_name"));
                }

            } catch (SQLException e) {
                System.err.println("ERROR Ошибка подключения к БД: " + e.getMessage());
                throw e;
            }
        }
        return connection;
    }
    public static List<Integer> getExistingIds(String tableName, String idColumn) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        String sql = "SELECT " + idColumn + " FROM " + tableName + " ORDER BY " + idColumn;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getInt(idColumn));
            }
        }
        return ids;
    }

}