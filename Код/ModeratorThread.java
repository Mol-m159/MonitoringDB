package imitator.threads;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ModeratorThread extends BaseImitationThread {
    private final RandomCollection<String> rc =new RandomCollection<String>()
            .add(40, "moderation")
            .add(1, "view_History");

    private static final String[] STATUSES = {"approved", "rejected"};
    public ModeratorThread(Connection connection, CurrentData currentData) {
        super(connection, currentData, "ModeratorThread");

    }
    private List<Integer> homebrewList;

    @Override
    public void run() {
        int countAct = 1 + random.nextInt(10);
        try {
            enter("moderator");
            check_Notif();
           homebrewList = selectModerationHomebrew();
           while (countAct > 0 && !homebrewList.isEmpty()){
                switch (rc.next()){
                    case "view_History":
                        viewHistory();
                        break;
                    default:
                        moderation();
                }
               countAct -=1;
           }
            Thread.sleep(10000);
            exit();
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    private void moderation() throws SQLException {
        if (homebrewList.isEmpty()) {
            System.out.println("Нет записей для модерации");
            return;
        }
        int randomIndex = random.nextInt(homebrewList.size());
        int entityIdFromList = homebrewList.remove(randomIndex);

        Map<Integer, String> resultMap = selectedHW(entityIdFromList);
        Map.Entry<Integer, String> entity = resultMap.entrySet().iterator().next();
        String decision = STATUSES[random.nextInt(STATUSES.length)];
        String date = currentData.toString();

        String insertSql = "INSERT INTO homebrew_moderations (moderator_id, entity_id, moderation_date, old_status, new_status ) \n" +
                "VALUES (?, ?,  CONVERT(datetime, ?, 120), ?, ?);";
        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setInt(1, userId);
            insertStmt.setInt(2, entity.getKey());
            insertStmt.setString(3, date);
            insertStmt.setString(4, entity.getValue());
            insertStmt.setString(5, decision);

            int rowsAffected = insertStmt.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Homebrew: " + entityIdFromList + " прошла модерацию. " + ". Дата: " + date);
                String updateSql = "UPDATE homebrew_entities SET status = ? WHERE entity_id = ?";
                try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
                    stmt.setString(1, decision);
                    stmt.setInt(2, entity.getKey());

                    int rowsUpdated = stmt.executeUpdate();
                    if (rowsUpdated  > 0) {
                        System.out.println("Статус Homebrew " + entity.getKey() + " обновлен");
                    }
                }
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }
    private Map<Integer, String> selectedHW(int entityId){
        String selectSql = "SELECT * FROM homebrew_entities WHERE entity_id = ?";
        Map<Integer, String> result = new HashMap<>();
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            stmt.setInt(1, entityId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    int id = rs.getInt("entity_id");
                    String str = rs.getString("status");
                    result.put(id, str);
                }
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private List<Integer> selectModerationHomebrew() {
        String selectSql = "SELECT *\n" +
                "FROM homebrew_entities he\n" +
                "WHERE he.status = 'moderation' \n" +
                "ORDER BY he.created_date ASC;";
        List<Integer> homebrewIDList = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            try (ResultSet rs = stmt.executeQuery()) {
                int count = 0;
                while (rs.next() && count < 100) {
                    int entityId = rs.getInt("entity_id");
                    homebrewIDList.add(entityId);
                    count++;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return homebrewIDList;
    }

    private void viewHistory(){
        String selectSql = "SELECT \n" +
                "    he.entity_id, he.entity_type, he.created_date as entity_created, u.user_id as author_id, u.registration_date as author_registered,\n" +
                "    gs.system_id, gs.created_date as system_created, he.status, COUNT(he_edits.edit_id) as edit_count,\n" +
                "    MAX(he_edits.edit_date) as last_edit_date, MAX(he_edits.version_number) as current_version,\n" +
                "    COUNT(hm.moderation_id) as moderation_count, MAX(hm.moderation_date) as last_moderation_date,\n" +
                "    MAX(CASE WHEN hm.new_status = 'approved' THEN hm.moderation_date END) as last_approval_date,\n" +
                "    MAX(CASE WHEN hm.new_status = 'rejected' THEN hm.moderation_date END) as last_rejection_date\n" +
                "FROM homebrew_entities he\n" +
                "JOIN users u ON he.author_id = u.user_id\n" +
                "JOIN game_systems gs ON he.system_id = gs.system_id\n" +
                "LEFT JOIN homebrew_edits he_edits ON he.entity_id = he_edits.entity_id\n" +
                "LEFT JOIN homebrew_moderations hm ON he.entity_id = hm.entity_id\n" +
                "WHERE he.status = 'moderation'\n" +
                "GROUP BY he.entity_id, he.entity_type, he.created_date, u.user_id, \n" +
                "         u.registration_date, gs.system_id, gs.created_date, he.status\n" +
                "ORDER BY \n" +
                "    DATEDIFF(DAY, u.registration_date, GETDATE()) ASC,\n" +
                "    he.created_date ASC;";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр общей истории модерации ");
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
