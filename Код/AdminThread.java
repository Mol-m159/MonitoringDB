package imitator.threads;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AdminThread extends BaseImitationThread {
    private final RandomCollection<String> rc =new RandomCollection<String>()
            .add(1, "userStatistic")
            .add(1, "sessionStatistic")
            .add(1, "viewHistory");

    private static final String[] STATUSES = {"approved", "rejected"};
    public AdminThread(Connection connection, CurrentData currentData) {
        super(connection, currentData, "AdminThread");

    }
    private List<Integer> homebrewList;

    @Override
    public void run() {
        int countAct = 1 + random.nextInt(10);
        try {
            enter("admin");
            check_Notif();
            switch (rc.next()){
                case "userStatistic":
                    userStatistic();
                    break;
                case "viewHistory":
                    viewHistory();
                    break;
                case "sessionStatistic":
                    sessionStatistic();
                    break;
                default:
                    systemStatistic();
            }

            Thread.sleep(100000);
            exit();

        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
    private void systemStatistic(){
        String selectSql = "SELECT  gs.system_id,\n" +
                "    COUNT(DISTINCT c.character_id) as character_count\n" +
                "FROM game_systems gs\n" +
                "LEFT JOIN characters c ON gs.system_id = c.system_id\n" +
                "GROUP BY gs.system_id;";
        String selectSqlFull = "SELECT  gs.system_id, gs.created_date, gs.is_active,\n" +
                "    COUNT(DISTINCT ev.view_id) as total_views,\n" +
                "    COUNT(DISTINCT he.entity_id) as homebrew_count,\n" +
                "    COUNT(DISTINCT c.character_id) as character_count,\n" +
                "    COUNT(DISTINCT c.user_id) as active_users\n" +
                "FROM game_systems gs\n" +
                "LEFT JOIN homebrew_entities he ON gs.system_id = he.system_id\n" +
                "LEFT JOIN characters c ON gs.system_id = c.system_id\n" +
                "LEFT JOIN entity_views ev ON he.entity_id = ev.entity_id  -- связь через homebrew_entities\n" +
                "WHERE gs.is_active = 1\n" +
                "GROUP BY gs.system_id, gs.created_date, gs.is_active\n" +
                "ORDER BY total_views DESC, homebrew_count DESC;";

        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр статистики систем ");
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void userStatistic(){
        String selectSql = "SELECT  u.user_id, gs.system_id, COUNT(DISTINCT c.character_id) as character_count,\n" +
                "    COUNT(DISTINCT he.entity_id) as homebrew_count, MAX(u.last_activity_date) as last_activity\n" +
                "FROM users u\n" +
                "LEFT JOIN characters c ON u.user_id = c.user_id\n" +
                "LEFT JOIN homebrew_entities he ON u.user_id = he.author_id\n" +
                "LEFT JOIN game_systems gs ON c.system_id = gs.system_id OR he.system_id = gs.system_id\n" +
                "WHERE gs.is_active = 1\n" +
                "GROUP BY u.user_id, gs.system_id\n" +
                "ORDER BY character_count DESC, homebrew_count DESC;";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр статистики пользователей ");
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void sessionStatistic(){
        String selectSql = "SELECT u.user_id, us.session_id, us.login_date, us.logout_date,\n" +
                "    DATEDIFF(SECOND, us.login_date, us.logout_date) as session_duration_seconds,\n" +
                "    DATEDIFF(MINUTE, us.login_date, us.logout_date) as session_duration_minutes,\n" +
                "    COUNT(DISTINCT c.character_id) as characters_edited,\n" +
                "    COUNT(DISTINCT ev.view_id) as entities_viewed\n" +
                "FROM user_sessions us\n" +
                "JOIN users u ON us.user_id = u.user_id\n" +
                "LEFT JOIN characters c ON u.user_id = c.user_id \n" +
                "    AND c.last_modified_date BETWEEN us.login_date AND us.logout_date\n" +
                "LEFT JOIN entity_views ev ON u.user_id = ev.user_id \n" +
                "    AND ev.view_date BETWEEN us.login_date AND us.logout_date\n" +
                "GROUP BY u.user_id, us.session_id, us.login_date, us.logout_date\n" +
                "ORDER BY session_duration_seconds DESC;";
        try (PreparedStatement stmt = connection.prepareStatement(selectSql)){
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("Просмотр статистики сессий ");
            }
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
