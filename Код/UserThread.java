package imitator.threads;

import imitator.DatabaseConnection;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class UserThread extends BaseImitationThread {
    private final RandomCollection<String> rc =new RandomCollection<String>()
            .add(15, "creation_Character")
            .add(40, "edit_Character")
            .add(10, "creation_Homebrew")
            .add(15, "view_Homebrew")
            .add(10, "registration")
            .add(10, "edit_Homebrew");
    private static final String[] CHARACTER_EDIT_TYPES = {// "creation" Создание персонажа
            "name_change",                 // Изменение имени
            "level_up",                    // Повышение уровня
            "ability_score_adjustment",    // Изменение характеристик
            "hit_points_update",           // Обновление HP
            "skill_selection",             // Выбор навыков
            "feat_selection",              // Выбор черт
            "class_change",                // Смена класса
            "race_change",                 // Смена расы
            "background_update",           // Изменение предыстории
            "alignment_change",            // Смена мировоззрения
            "experience_adjustment"        // Корректировка опыта
    };
    private static final String[] ENTITY_TYPES = {"spell", "item", "class", "race", "ability"};
    public UserThread(Connection connection, CurrentData currentData) {
        super(connection, currentData, "UserThread");

    }

    @Override
    public void run() {
        String act = rc.next();
        try {
            if(act.equals("registration")){
                try {
                    registration();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            } else{enter("user");
                check_Notif();
                switch (act) {
                    case "creation_Character":
                        creation_Character();
                        break;
                    case "edit_Character":
                        edit_Character();
                        break;
                    case "creation_Homebrew":
                        creation_Homebrew();
                        break;
                    case "view_Homebrew":
                        view_Homebrew();
                        break;
                    case "edit_Homebrew":
                        edit_Homebrew();
                        break;
                    default:
                        System.err.println("Неопознанный тип деятельности.");
                }
                Thread.sleep(1000);
                exit();

            }

        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void registration() throws SQLException {
        String date = currentData.toString();
        String insertSql = "INSERT INTO users (registration_date, last_activity_date) \n" +
                "VALUES ( \n" +
                "    CONVERT(datetime, ?, 120),\n" +
                "    CONVERT(datetime, ?, 120)\n" +
                ");";
        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            insertStmt.setString(1, date);
            insertStmt.setString(2, date);

            int rowsAffected = insertStmt.executeUpdate();
            if (rowsAffected > 0) {
                try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        System.out.println("Пользователь зарегистрирован: " + generatedKeys.getLong(1) + ". Дата регистрации: " + date);
                    }
                }
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private void creation_Character(){
        String insertSql = "INSERT INTO characters (user_id, system_id, created_date, last_modified_date) \n" +
                "VALUES (?, ?, CONVERT(datetime, ?, 120), CONVERT(datetime, ?, 120) " +
                ");";
        String date = currentData.toString();
        try {
            List<Integer> systemIDList = DatabaseConnection.getExistingIds("game_systems", "system_id");
            if (!systemIDList.isEmpty()) {
                int system_id = systemIDList.get(random.nextInt(systemIDList.size()));
                try (PreparedStatement insertStmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                    insertStmt.setString(1, Integer.toString(userId));
                    insertStmt.setString(2, Integer.toString(system_id));
                    insertStmt.setString(3, date);
                    insertStmt.setString(4, date);

                    int rowsAffected = insertStmt.executeUpdate();
                    if (rowsAffected > 0) {
                        try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                long id = generatedKeys.getLong(1);
                                System.out.println("Добавлен персонаж: " + id + ". Дата создания: " + date );
                                String insertSql1 = "INSERT INTO character_edits (character_id, edit_date, edit_type)" +
                                        "                VALUES ( ?, CONVERT(datetime, ?, 120), ?);";
                                try (PreparedStatement insertStmt1 = connection.prepareStatement(insertSql1)) {
                                    insertStmt1.setLong(1, id);
                                    insertStmt1.setString(2, date);
                                    insertStmt1.setString(3, "creation");
                                    int rowsAffected1 = insertStmt1.executeUpdate();
                                    if (rowsAffected1 > 0) {
                                        System.out.println("Добавлено редактирование персонажа: " + id + ". Дата редактирования: " + date + ". Тип изменения: " + "creation");
                                    }
                                } catch (SQLException e) {
                                    connection.rollback();
                                    throw e;
                                }
                            }
                        }
                    }
                    connection.commit();
                } catch (SQLException e) {
                    connection.rollback();
                    throw e;
                }
            } else System.out.println("Нет пользователей или игровых систем");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void edit_Character() throws SQLException {
        List<Integer> characterIDList = select_Character(userId);
        if (characterIDList.isEmpty()){
            System.err.println("У пользователя нет персонажей");
        }else{
            String date = currentData.toString();
            String edit_type = CHARACTER_EDIT_TYPES[random.nextInt(CHARACTER_EDIT_TYPES.length)];
            String insertSql = "INSERT INTO character_edits (character_id, edit_date, edit_type) \n" +
                    "VALUES ( ?, CONVERT(datetime, ?, 120), ?);";
            int character_id = characterIDList.get(random.nextInt(characterIDList.size()));
            editCharacterHistory(character_id);
            try (PreparedStatement insertStmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                insertStmt.setInt(1, character_id);
                insertStmt.setString(2, date);
                insertStmt.setString(3, edit_type);

                int rowsAffected = insertStmt.executeUpdate();
                if (rowsAffected > 0) {
                    try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            System.out.println("Добавлено редактирование персонажа: " + generatedKeys.getLong(1) + ". Дата редактирования: " + date + ". Тип изменения: " + edit_type);
                            String updateSql = "UPDATE characters SET last_modified_date = CONVERT(datetime, ?, 120) WHERE character_id = ?";
                            try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
                                stmt.setString(1, date);
                                stmt.setInt(2, character_id);

                                int rowsUpdated = stmt.executeUpdate();
                                if (rowsUpdated == 0) {
                                    throw new SQLException("Персонаж с ID " + character_id + " не найден в таблице characters");
                                }
                                System.out.println("Обновлена last_modified_date для персонажа: " + character_id);
                            }
                        }
                    }
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private void creation_Homebrew(){
        String date = currentData.toString();
        String insertSql = "INSERT INTO homebrew_entities (author_id, system_id, entity_type, created_date, status) \n" +
                "VALUES (?, ?, ?, CONVERT(datetime, ?, 120), ?);";

        try {
            List<Integer> systemIDList = DatabaseConnection.getExistingIds("game_systems", "system_id");
            if (!systemIDList.isEmpty()) {
                int system_id = systemIDList.get(random.nextInt(systemIDList.size()));
                String edit_type = ENTITY_TYPES[random.nextInt(ENTITY_TYPES.length)];
                String status = "draft";
                try (PreparedStatement insertStmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                    insertStmt.setInt(1, userId);
                    insertStmt.setInt(2, system_id);
                    insertStmt.setString(3, edit_type);
                    insertStmt.setString(4, date);
                    insertStmt.setString(5, status);

                    int rowsAffected = insertStmt.executeUpdate();
                    if (rowsAffected > 0) {
                        try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                int id = generatedKeys.getInt(1);
                                System.out.println("Добавлено homebrew: " + id + ". Автор: " + userId +". Тип: " + edit_type + ". Дата создания: " + date + ". Статус: " + status);
                                String insertSql1 = "INSERT INTO homebrew_edits (entity_id, edit_date, version_number)" +
                                        "                VALUES ( ?, CONVERT(datetime, ?, 120), ?);";
                                try (PreparedStatement insertStmt1 = connection.prepareStatement(insertSql1)) {
                                    insertStmt1.setInt(1, id);
                                    insertStmt1.setString(2, date);
                                    insertStmt1.setInt(3, 1);
                                    int rowsAffected1 = insertStmt1.executeUpdate();
                                    if (rowsAffected1 > 0) {
                                        System.out.println("Добавлено редактирование homebrew " + id + ". Дата редактирования: " + date);
                                    }
                                } catch (SQLException e) {
                                    connection.rollback();
                                    throw e;
                                }
                            }
                        }
                    }
                    connection.commit();
                } catch (SQLException e) {
                    connection.rollback();
                    throw e;
                }
            } else System.out.println("Нет игровых систем");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void edit_Homebrew() throws SQLException {
        List<Integer> homebrewIDList = select_Homebrew(userId);
        if (homebrewIDList.isEmpty()){
            System.err.println("У пользователя нет homebrew");
        }else{
            String date = currentData.toString();
            String insertSql = "INSERT INTO homebrew_edits (entity_id, edit_date, version_number)" +
                    "VALUES ( ?, CONVERT(datetime, ?, 120), ?);";
            String ver = String.valueOf(random.nextInt(10)+1);
            int entity_id = homebrewIDList.get(random.nextInt(homebrewIDList.size()));
            editHomebrewHistory(entity_id);
            viewHomebrewCount(entity_id);
            try (PreparedStatement insertStmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                insertStmt.setInt(1, entity_id);
                insertStmt.setString(2, date);
                insertStmt.setString(3, ver);

                int rowsAffected = insertStmt.executeUpdate();
                if (rowsAffected > 0) {
                        System.out.println("Добавлено редактирование homebrew: " + entity_id + ". Дата редактирования: " + date);
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private void view_Homebrew(){
        String date = currentData.toString();
        String insertSql = "INSERT INTO entity_views (user_id, entity_id, view_date) \n" +
                "VALUES (?, ?, CONVERT(datetime, ?, 120));";
        try {
            List<Integer> entityIDList = DatabaseConnection.getExistingIds("homebrew_entities", "entity_id");
            if (!entityIDList.isEmpty()) {
                int entity_id = entityIDList.get(random.nextInt(entityIDList.size()));
                try (PreparedStatement insertStmt = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
                    insertStmt.setInt(1, userId);
                    insertStmt.setInt(2, entity_id);
                    insertStmt.setString(3, date);

                    int rowsAffected = insertStmt.executeUpdate();
                    if (rowsAffected > 0) {
                        try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                long id = generatedKeys.getLong(1);
                                System.out.println("Добавлен просмотр: " + id + ". Пользователь: " + userId +". homebrew: " + entity_id  + ". Дата: " + date);
                            }
                        }
                    }
                    connection.commit();
                } catch (SQLException e) {
                    connection.rollback();
                    throw e;
                }
            } else System.out.println("Нет пользовательского контента");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private List<Integer> select_Character(int id) {
        String selectSql = "SELECT * FROM characters WHERE user_id = ?";
        List<Integer> characterIDList = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
                stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    int characterId = rs.getInt("character_id");
                    characterIDList.add(characterId);
                }
            }
        }catch (SQLException e) {
                throw new RuntimeException(e);
            }
        return characterIDList;
    }

    private List<Integer> select_Homebrew(int id) {
        String selectSql = "SELECT * FROM homebrew_entities WHERE author_id = ?";
        List<Integer> homebrewIDList = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            stmt.setInt(1, id);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        int characterId = rs.getInt("entity_id");
                        homebrewIDList.add(characterId);
                    }
                }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return homebrewIDList;
    }

    private void editCharacterHistory(int id){
        String selectSql = "SELECT *\n" +
                "FROM character_edits ce\n" +
                "WHERE ce.character_id = ? \n" +
                "ORDER BY ce.edit_date DESC;";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр истории изменения персонажа " + id);
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void editHomebrewHistory(int id){
        String selectSql = "SELECT *\n" +
                "   FROM homebrew_edits\n" +
                "  WHERE entity_id = ?\n" +
                "  Order BY version_number DESC";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр истории изменения Homebrew " + id);
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void viewHomebrewCount(int id){
        String selectSql = "SELECT \n" +
                "      COUNT(view_id) as countView\n" +
                "  FROM entity_views\n" +
                "  WHERE entity_id = ?\n" +
                "  GROUP BY entity_id";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            stmt.setInt(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр количества просмотров Homebrew " + id);
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
