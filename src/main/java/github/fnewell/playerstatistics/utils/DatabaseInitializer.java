package github.fnewell.playerstatistics.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


public class DatabaseInitializer {

    /*
     * Initializes the database with the necessary tables.
     * @param connection The connection to the database.
     * @param dbType The type of the database.
     */
    public static void initializeDatabase(Connection connection, String dbType) {
        // Define the id column definition based on the database type
        String idDefinition = (dbType.equalsIgnoreCase("SQLITE")) ? "INTEGER PRIMARY KEY" : "INT NOT NULL AUTO_INCREMENT PRIMARY KEY";

        String[] createTableStatements = {
                // Table uuid_map
                "CREATE TABLE IF NOT EXISTS uuid_map (" +
                        "id " + idDefinition + "," +
                        "player_uuid " + getUUIDType(dbType) + " NOT NULL UNIQUE," +
                        "player_nick VARCHAR(16) DEFAULT NULL" +
                        ")",

                // Table sync_metadata
                "CREATE TABLE IF NOT EXISTS sync_metadata (" +
                        "last_update " + getTimestampType(dbType) + " DEFAULT NULL," +
                        "PRIMARY KEY (last_update)" +
                        ")",

                // Table hall_of_fame
                "CREATE TABLE IF NOT EXISTS hall_of_fame (" +
                        "player_id INT NOT NULL," +
                        "first_place INT NOT NULL," +
                        "second_place INT NOT NULL," +
                        "third_place INT NOT NULL," +
                        "fourth_place INT NOT NULL," +
                        "fifth_place INT NOT NULL," +
                        "score INT NOT NULL," +
                        "PRIMARY KEY (player_id)," +
                        "FOREIGN KEY (player_id) REFERENCES uuid_map(id) ON DELETE CASCADE" +
                        ")",

                // Tables for statistics
                createStatsTableSQL("broken", dbType),
                createStatsTableSQL("crafted", dbType),
                createStatsTableSQL("custom", dbType),
                createStatsTableSQL("dropped", dbType),
                createStatsTableSQL("killed", dbType),
                createStatsTableSQL("killed_by", dbType),
                createStatsTableSQL("mined", dbType),
                createStatsTableSQL("picked_up", dbType),
                createStatsTableSQL("used", dbType)
        };

        try {
            for (String sql : createTableStatements) {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.execute();
                }
            }

            // Check if the last_update is initialized
            if (!isLastUpdateInitialized(connection)) {
                initializeLastUpdate(connection, dbType);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
     * Creates the SQL statement for creating a statistics table.
     * The table uses player_id as a foreign key referencing uuid_map.
     * @param tableName The name of the table.
     * @param dbType The type of the database.
     */
    private static String createStatsTableSQL(String tableName, String dbType) {
        return "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" +
                "player_id INT NOT NULL," +
                "position INT(2) NULL," +
                "stat_name VARCHAR(256) NOT NULL," +
                "amount INT NOT NULL," +
                "PRIMARY KEY (player_id, stat_name)," +
                "FOREIGN KEY (player_id) REFERENCES uuid_map(id) ON DELETE CASCADE," +
                "INDEX idx_position (position)" +
                ")";
    }

    /*
     * Returns the appropriate timestamp type for the database.
     * @param dbType The type of the database.
     * @return The timestamp type.
     */
    private static String getTimestampType(String dbType) {
        return switch (dbType) {
            case "MARIADB", "MYSQL" -> "DATETIME";
            case "POSTGRESQL" -> "TIMESTAMP";
            case "SQLITE" -> "TEXT";
            default -> throw new IllegalArgumentException("Not supported database type (getTimestampType): " + dbType);
        };
    }

    /*
     * Returns the appropriate UUID type for the database.
     * @param dbType The type of the database.
     * @return The UUID type.
     */
    private static String getUUIDType(String dbType) {
        return switch (dbType) {
            case "MARIADB", "MYSQL", "SQLITE" -> "VARCHAR(36)";
            case "POSTGRESQL" -> "UUID";
            default -> throw new IllegalArgumentException("Not supported database type (getUUIDType): " + dbType);
        };
    }

    /*
     * Checks if the last_update is initialized in the database.
     * @param connection The connection to the database.
     * @return True if the last_update is initialized, false otherwise.
     */
    private static boolean isLastUpdateInitialized(Connection connection) {
        String sql = "SELECT COUNT(*) FROM sync_metadata WHERE last_update IS NOT NULL";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            System.err.println("Error checking if last_update is initialized:");
            e.printStackTrace();
        }
        return false;
    }

    /*
     * Initializes the last_update in the database.
     * @param connection The connection to the database.
     * @param dbType The type of the database.
     */
    private static void initializeLastUpdate(Connection connection, String dbType) {
        String sql = "INSERT INTO sync_metadata (last_update) VALUES (?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            // Set the last_update to epoch time
            if ("SQLITE".equalsIgnoreCase(dbType)) {
                statement.setString(1, "1970-01-01 00:00:00");
            } else {
                statement.setTimestamp(1, new Timestamp(0));
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error initializing last_update:");
            e.printStackTrace();
        }
    }
}