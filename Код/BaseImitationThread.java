package imitator.threads;

import imitator.DatabaseConnection;
import imitator.ImitatorConfig;

import java.sql.*;
import java.time.LocalDateTime;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public abstract class BaseImitationThread extends Thread {
    protected final Connection connection;
    protected CurrentData currentData;
    protected final String threadName;
    protected final Random random;
    protected int userId = 0;
    private final CountDownLatch loginCompletedLatch = new CountDownLatch(1);

    public BaseImitationThread(Connection dbConnection,
                               CurrentData currentData, String name) {
        this.connection = dbConnection;
        this.currentData = currentData;
        this.threadName = name;
        this.random = new Random();
        this.setName(name);
    }
    protected void enter(String type) throws SQLException {
        try {

            selectUser(type);

            int attempts = 0;
            while (openSession(this.userId) && attempts < 10) { // ограничим попытки
                System.out.println("Сессия для пользователя " + userId + " уже открыта, выбираем другого");
                selectUser(type);
                attempts++;
            }

            if (attempts >= 10) {
                throw new SQLException("Не удалось найти пользователя с закрытой сессией после 10 попыток");
            }

            performLogin();
            System.out.println("Успешный вход пользователя " + userId + ". Роль: " + type);
        } finally {
            loginCompletedLatch.countDown();
        }
    }

    private void selectUser(String type) throws SQLException {
        switch (type) {
            case "admin":
                userId = ImitatorConfig.ADMINS_ID[random.nextInt(ImitatorConfig.ADMINS_ID.length)];
                break;
            case "moderator":
                userId = ImitatorConfig.MODERS_ID[random.nextInt(ImitatorConfig.MODERS_ID.length)];
                break;
            default:
                userId = selectRegularUser();
                break;
        }

        if (userId == 0) {
            throw new SQLException("Не удалось выбрать пользователя для типа: " + type);
        }
    }

    private int selectRegularUser() throws SQLException {
        Map<Integer, Timestamp[]> result = new HashMap<>();
        String sql = "SELECT user_id, registration_date, last_activity_date FROM users";

        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int userId = rs.getInt("user_id");
                Timestamp ts1 = rs.getTimestamp("registration_date");
                Timestamp ts2 = rs.getTimestamp("last_activity_date");
                result.put(userId, new Timestamp[]{ts1, ts2});
            }

            return getRandomUserId(result);
        }
    }

    private void performLogin() throws SQLException {
        String date = currentData.toString();

        try {
            updateUserLastActivity(date);
            createUserSession(date);
            connection.commit();

        } catch (SQLException e) {
            connection.rollback();
            System.err.println("Ошибка при входе, транзакция откатана: " + e.getMessage());
            throw e;
        }
    }

    private void updateUserLastActivity(String date) throws SQLException {
        String updateSql = "UPDATE users SET last_activity_date = CONVERT(datetime, ?, 120) WHERE user_id = ?";

        try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
            stmt.setString(1, date);
            stmt.setInt(2, userId);

            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("Пользователь с ID " + userId + " не найден в таблице users");
            }
            System.out.println("Обновлена last_activity_date для пользователя: " + userId);
        }
    }

    private void createUserSession(String date) throws SQLException {
        String insertSql = "INSERT INTO user_sessions (user_id, login_date, logout_date) VALUES (?, CONVERT(datetime, ?, 120), NULL)";

        try (PreparedStatement stmt = connection.prepareStatement(insertSql)) {
            stmt.setInt(1, userId);
            stmt.setString(2, date);

            int rowsInserted = stmt.executeUpdate();
            System.out.println("Создана новая сессия для пользователя: " + userId + ", строк вставлено: " + rowsInserted);
        }
    }

    protected void exit() throws SQLException {
        try {
            loginCompletedLatch.await(30, TimeUnit.SECONDS); // ждем до 30 секунд
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Ожидание входа прервано", e);
        }
        if (userId == 0) {
            System.err.println("Попытка выхода без установленного userId");
            return;
        }

        String date = currentData.toString();

        try {
            closeUserSession(date);
            connection.commit();

        } catch (SQLException e) {
            connection.rollback();
            System.err.println("Ошибка при выходе, транзакция откатана: " + e.getMessage());
            throw e;
        }

        System.out.println("Успешный выход пользователя: " + userId);
    }

    private void closeUserSession(String date) throws SQLException {
        String updateSql = "UPDATE user_sessions SET logout_date = CONVERT(datetime, ?, 120) WHERE user_id = ? AND logout_date IS NULL";

        try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
            stmt.setString(1, date);
            stmt.setInt(2, userId);

            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                System.err.println("Не найдено открытых сессий для пользователя: " + userId);
            } else {
                System.out.println("Закрыто сессий для пользователя " + userId + ": " + rowsUpdated);
            }
        }
    }
    private Integer getRandomUserId(Map<Integer, Timestamp[]> usersMap) {
        if (usersMap == null || usersMap.isEmpty()) {
            return 0;
        }
        List<Integer> userIds = new ArrayList<>(usersMap.keySet());
        int i =  userIds.get(random.nextInt(userIds.size()));
        while (isIdInAnyArrays(i)){
            i =  userIds.get(random.nextInt(userIds.size()));
        }
        return i;
    }
    private boolean isIdInAnyArrays(int id) {
        return Arrays.stream(ImitatorConfig.ADMINS_ID).anyMatch(x -> x == id)  ||
                Arrays.stream(ImitatorConfig.MODERS_ID).anyMatch(x -> x == id);
    }
    private boolean openSession(int id){
        Set<Integer> set = new HashSet<>();
        String sql = "SELECT user_id FROM user_sessions WHERE logout_date is NULL";
        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                int userId = rs.getInt("user_id");
                set.add(userId);
            }
            return set.contains(id);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    protected void check_Notif(){
        String selectSql = "SELECT  u.user_id, n.notification_id, n.created_date as notification_created,\n" +
                "    n.notification_type, ne.event_date, ne.event_type\n" +
                "FROM notifications n\n" +
                "JOIN users u ON n.user_id = u.user_id\n" +
                "LEFT JOIN notification_events ne ON n.notification_id = ne.notification_id\n" +
                "WHERE u.user_id = ? \n" +
                "ORDER BY n.created_date DESC, ne.event_date DESC;";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            stmt.setInt(1, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр уведомлений пользователя " + userId);
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
