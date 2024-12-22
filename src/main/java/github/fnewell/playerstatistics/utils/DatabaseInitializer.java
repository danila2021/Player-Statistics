package github.fnewell.playerstatistics.utils;

import github.fnewell.playerstatistics.PlayerStatistics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


public class DatabaseInitializer {

    /**
      * Initializes the database with the necessary tables.
      * @param connection The connection to the database.
      * @param dbType The type of the database.
      */
    public static void initializeDatabase(Connection connection, String dbType) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Initializing database ... \n ({}, {})", connection, dbType); }

        // Define the id column definition based on the database type
        String idDefinition = (dbType.equalsIgnoreCase("SQLITE")) ? "INTEGER PRIMARY KEY" : "INT NOT NULL AUTO_INCREMENT PRIMARY KEY";

        // Charset for MySQL/MariaDB
        String charset = (dbType.equalsIgnoreCase("MYSQL") || dbType.equalsIgnoreCase("MARIADB"))
                ? " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                : "";

        String[] createTableStatements = {
                // Table uuid_map
                "CREATE TABLE IF NOT EXISTS uuid_map (" +
                        "id " + idDefinition + "," +
                        "player_uuid " + getUUIDType(dbType) + " NOT NULL UNIQUE," +
                        "player_nick VARCHAR(16) DEFAULT NULL," +
                        "player_last_online " + getTimestampType(dbType) + " DEFAULT NULL" +
                        ")" + charset,

                // Table sync_metadata
                "CREATE TABLE IF NOT EXISTS sync_metadata (" +
                        "last_update " + getTimestampType(dbType) + " DEFAULT NULL," +
                        "server_name VARCHAR(256) DEFAULT NULL," +
                        "server_desc VARCHAR(256) DEFAULT NULL," +
                        "server_url VARCHAR(256) DEFAULT NULL," +
                        "server_icon BLOB DEFAULT NULL," +
                        "PRIMARY KEY (last_update)" +
                        ")" + charset,

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
                        ")" + charset,

                // Tables for statistics
                createStatsTableSQL("broken", charset),
                "CREATE INDEX IF NOT EXISTS idx_broken_position ON `broken` (position)",
                createStatsTableSQL("crafted", charset),
                "CREATE INDEX IF NOT EXISTS idx_crafted_position ON `crafted` (position)",
                createStatsTableSQL("custom", charset),
                "CREATE INDEX IF NOT EXISTS idx_custom_position ON `custom` (position)",
                createStatsTableSQL("dropped", charset),
                "CREATE INDEX IF NOT EXISTS idx_dropped_position ON `dropped` (position)",
                createStatsTableSQL("killed", charset),
                "CREATE INDEX IF NOT EXISTS idx_killed_position ON `killed` (position)",
                createStatsTableSQL("killed_by", charset),
                "CREATE INDEX IF NOT EXISTS idx_killed_by_position ON `killed_by` (position)",
                createStatsTableSQL("mined", charset),
                "CREATE INDEX IF NOT EXISTS idx_mined_position ON `mined` (position)",
                createStatsTableSQL("picked_up", charset),
                "CREATE INDEX IF NOT EXISTS idx_picked_up_position ON `picked_up` (position)",
                createStatsTableSQL("used", charset),
                "CREATE INDEX IF NOT EXISTS idx_used_position ON `used` (position)"
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
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Error initializing database: {}", e.getMessage());
        }
    }

    /**
      * Creates the SQL statement for creating a statistics table.
      * The table uses player_id as a foreign key referencing uuid_map.
      * @param tableName The name of the table.
      */
    private static String createStatsTableSQL(String tableName, String charset) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Creating table: {}", tableName); }

        // return createTableSQL + ";" + createIndexSQL;
        return "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" +
                "player_id INT NOT NULL," +
                "position INT(2) NULL," +
                "stat_name VARCHAR(256) NOT NULL," +
                "amount INT NOT NULL," +
                "PRIMARY KEY (player_id, stat_name)," +
                "FOREIGN KEY (player_id) REFERENCES uuid_map(id) ON DELETE CASCADE" +
                ")" + charset;
    }

    /**
      * Returns the appropriate timestamp type for the database.
      * @param dbType The type of the database.
      * @return The timestamp type.
      */
    private static String getTimestampType(String dbType) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Getting timestamp type for: {}", dbType); }

        return switch (dbType) {
            case "MARIADB", "MYSQL" -> "DATETIME";
            case "POSTGRESQL" -> "TIMESTAMP";
            case "SQLITE" -> "TEXT";
            default -> throw new IllegalArgumentException("Not supported database type (getTimestampType): " + dbType);
        };
    }

    /**
      * Returns the appropriate UUID type for the database.
      * @param dbType The type of the database.
      * @return The UUID type.
      */
    private static String getUUIDType(String dbType) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Getting UUID type for: {}", dbType); }

        return switch (dbType) {
            case "MARIADB", "MYSQL", "SQLITE" -> "VARCHAR(36)";
            case "POSTGRESQL" -> "UUID";
            default -> throw new IllegalArgumentException("Not supported database type (getUUIDType): " + dbType);
        };
    }

    /**
      * Checks if the last_update is initialized in the database.
      * @param connection The connection to the database.
      * @return True if the last_update is initialized, false otherwise.
      */
    private static boolean isLastUpdateInitialized(Connection connection) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Checking if last_update is initialized ..."); }

        String sql = "SELECT COUNT(*) FROM sync_metadata WHERE last_update IS NOT NULL";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Error checking if last_update is initialized: {}", e.getMessage());
        }
        return false;
    }

    /**
      * Initializes the last_update in the database.
      * @param connection The connection to the database.
      * @param dbType The type of the database.
      */
    private static void initializeLastUpdate(Connection connection, String dbType) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Initializing last_update ..."); }

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
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Error initializing last_update: {}", e.getMessage());
        }
    }
}