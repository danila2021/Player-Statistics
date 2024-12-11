package github.fnewell.playerstatistics.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static github.fnewell.playerstatistics.utils.StatSyncTask.getDatabaseConnection;

public class DatabaseUtils {

    public static List<Map<String, Object>> searchPlayerStats(String query) {
        List<Map<String, Object>> results = new ArrayList<>();
        String sql = "SELECT player_uuid, stat_name, amount FROM `minecraft:mined` WHERE player_uuid LIKE ?";

        try (Connection connection = getDatabaseConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, "%" + query + "%");
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("player", rs.getString("player_uuid"));
                    row.put("statName", rs.getString("stat_name"));
                    row.put("amount", rs.getInt("amount"));
                    results.add(row);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }
}
