package imitator;

import imitator.threads.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;


public class DataGenerator {
    private final Connection dbConnection;
    private final ExecutorService executor;
    private final Random random;
    private final List<BaseImitationThread> threads;
    private final AtomicInteger totalGeneratedRecords;
    private final RandomCollection<String> rc;
    private final CurrentData currentData;

    public DataGenerator(Connection dbConnection) {
        this.dbConnection = dbConnection;
        this.executor = Executors.newFixedThreadPool(ImitatorConfig.THREAD_POOL_SIZE);
        this.threads = new ArrayList<>();
        this.totalGeneratedRecords = new AtomicInteger(0);
        this.random = new Random();
        this.rc = new RandomCollection<String>()
                .add(90, "user")
                .add(9, "moderator")
                .add(1, "admin");
        this.currentData = new CurrentData(ImitatorConfig.START_DATE);
    }

    public void startImitation() {
        System.out.println("Запуск генерации данных...");
        startTime();
        startProgressMonitor();
        scheduleShutdown();
        generateUsers();
        notificationChanges();
    }

    private void startTime() {
        new Thread(() -> {
            while (!executor.isShutdown()) {
                try {
                    currentData.run();
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    private void notificationChanges() {
        new Thread(() -> {
            String[] DELIVERY_EVENTS = {
                    "created", "queued", "sent", "delivered", "failed", "bounced"
            };
            String[] NOTIFICATION_TYPES = {
                    "homebrew_approved", "homebrew_rejected", "homebrew_needs_changes",
                    "data_export_ready","password_changed",
                    "welcome_message",
                    "newsletter",
                    "system_announcement",
                    "bug_fix_notice",
                    "survey_invitation"
            };
            while (!executor.isShutdown()) {
                try {
                    try {
                        List<Integer> notificationsIDList = DatabaseConnection.getExistingIds("notifications", "notification_id");
                        if (!notificationsIDList.isEmpty()) {
                            int notification_id = notificationsIDList.get(random.nextInt(notificationsIDList.size()));
                            String event_type = DELIVERY_EVENTS[random.nextInt(DELIVERY_EVENTS.length)];
                            String date = currentData.toString();
                            if(event_type == "created"){
                                String insertSql1 = "INSERT INTO notifications (user_id, created_date, notification_type) \n" +
                                        "VALUES (?, CONVERT(datetime, ?, 120), ?)";
                                String notification_type = NOTIFICATION_TYPES[random.nextInt(NOTIFICATION_TYPES.length)];
                                List<Integer> usersIDList = DatabaseConnection.getExistingIds("users", "user_id");
                                if (!usersIDList.isEmpty()) {
                                    int user_id = usersIDList.get(random.nextInt(usersIDList.size()));
                                    try (PreparedStatement insertStmt = dbConnection.prepareStatement(insertSql1, Statement.RETURN_GENERATED_KEYS)) {
                                        insertStmt.setInt(1, user_id);
                                        insertStmt.setString(2, date);
                                        insertStmt.setString(3, notification_type);

                                        int rowsAffected = insertStmt.executeUpdate();
                                        if (rowsAffected > 0) {
                                            try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                                                if (generatedKeys.next()) {
                                                    long id = generatedKeys.getLong(1);
                                                    System.out.println("Добавлено уведомление: " + id + ". Дата создания: " + date + ". Тип: " + notification_type);
                                                }
                                            }
                                        }
                                        dbConnection.commit();
                                    } catch (SQLException e) {
                                        dbConnection.rollback();
                                        throw e;
                                    }
                                } else System.out.println("Нет пользователей");
                            }
                            String insertSql = "INSERT INTO notification_events (notification_id, event_date, event_type) \n" +
                                    "VALUES (?, CONVERT(datetime, ?, 120), ?);";
                            try (PreparedStatement insertStmt = dbConnection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                                insertStmt.setInt(1, notification_id);
                                insertStmt.setString(3, event_type);
                                insertStmt.setString(2, date);

                                int rowsAffected = insertStmt.executeUpdate();
                                if (rowsAffected > 0) {
                                    try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                                        if (generatedKeys.next()) {
                                            long id = generatedKeys.getLong(1);
                                            System.out.println("Добавлено событие уведомления: " + id + ". Уведомление : " + notification_id +". Дата : " + date + ". Тип: " + event_type);
                                        }
                                    }
                                }
                                dbConnection.commit();
                            } catch (SQLException e) {
                                dbConnection.rollback();
                                throw e;
                            }
                        } else System.out.println("Нет уведомлений");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    private void startProgressMonitor() {
        new Thread(() -> {
            while (!executor.isShutdown()) {
                try {
                    Thread.sleep(5000);
                    System.out.println("Текущая дата: " + currentData.toString());
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    private void generateUsers() {
        new Thread(() -> {
            while (!executor.isShutdown()) {
                try {
                    BaseImitationThread task;
                    switch (rc.next()) {
                        case "user":
                            task = new UserThread(dbConnection, currentData);
                            threads.add(task);
                            executor.execute(task);
                            break;
                        case "moderator":
                            task = new ModeratorThread(dbConnection, currentData);
                            threads.add(task);
                            executor.execute(task);
                            break;
                        case "admin":
                            task = new AdminThread(dbConnection, currentData);
                            threads.add(task);
                            executor.execute(task);
                            break;
                        default:
                            task = new NullThread(dbConnection, currentData);
                            threads.add(task);
                            executor.execute(task);
                    }

                    Thread.sleep(10);

                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    private void scheduleShutdown() {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                stopImitation();
            }
        }, ImitatorConfig.GENERATION_DURATION_MINUTES * 60 * 1000);
    }

    public void stopImitation() {
        System.out.println("Остановка генерации...");

        threads.forEach(Thread::interrupt);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        System.out.println("Всего записей: " + totalGeneratedRecords.get());

        if (dbConnection != null) {
            try {
                dbConnection.close();
                System.out.println("Соединение с БД закрыто");
            } catch (Exception closeEx) {
                System.err.println("Ошибка при закрытии соединения: " + closeEx.getMessage());
            }
        }
        System.out.println("---Имитация деятельности завершена ---");
    }
}
