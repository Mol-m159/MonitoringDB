import util.Config;

import imitator.DataGenerator;
import imitator.DatabaseConnection;


import java.sql.Connection;

public class Main {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            System.out.println("=== Имитация деятельности ===");

            Config config = new Config();
            System.out.println("Конфигурация загружена");
            connection = DatabaseConnection.getConnection();
            connection.setAutoCommit(false);
            System.out.println("Auto-commit установлен в false");


            System.out.println("\n--- Начало деятельности ---");
            DataGenerator generator = new DataGenerator(connection);
            generator.startImitation();


        } catch (Exception e) {
            System.err.println("ERROR Произошла ошибка: " + e.getMessage());
            e.printStackTrace();

            if (connection != null) {
                try {
                    connection.rollback();
                    System.out.println("Транзакция откатана из-за ошибки");
                } catch (Exception rollbackEx) {
                    System.err.println("Ошибка при откате транзакции: " + rollbackEx.getMessage());
                }
            }
        }
    }
}