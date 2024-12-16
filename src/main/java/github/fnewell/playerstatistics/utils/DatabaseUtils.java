package github.fnewell.playerstatistics.utils;

import com.fasterxml.jackson.databind.JsonNode;
import github.fnewell.playerstatistics.localdatabase.LocalDatabase;

import java.net.HttpURLConnection;
import java.net.URI;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DatabaseUtils {

    // Database location (LOCAL or REMOTE)
    public static final String DB_LOCATION = ConfigUtils.config.getString("database.location");
    private static final String DB_TYPE = ConfigUtils.config.getString("database.type");

    private static final List<String> TABLE_NAMES = Arrays.asList(
        "broken", "crafted", "custom", "dropped", "killed", "killed_by", "mined", "picked_up", "used"
    );


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

    /**
     * Get a connection to the database based on the configuration.
     * This method is used to connect to a local or remote database based on the configuration.
     *
     * @return The database connection.
     * @throws Exception If an error occurs while connecting to the database.
     */
    public static Connection getDatabaseConnection() throws Exception {
        // Connect to the database based on the configuration
        if ("REMOTE".equals(DB_LOCATION)) {
            String DbType = ConfigUtils.config.getString("database.type");
            String DbName = ConfigUtils.config.getString("database.name");
            String DbHost = ConfigUtils.config.getString("database.host");
            String DbPort = ConfigUtils.config.getString("database.port");
            String DbUser = ConfigUtils.config.getString("database.username");
            String DbPassword = ConfigUtils.config.getString("database.password");

            // Construct the JDBC URL based on the database type
            String url = switch (DbType) {
                case "MARIADB", "MYSQL", "POSTGRESQL" ->
                        "jdbc:" + DbType.toLowerCase() + "://" + DbHost + ":" + DbPort + "/" + DbName;
                case "SQLITE" -> "jdbc:sqlserver://" + DbHost + ":" + DbPort + ";databaseName=" + DbName;
                default -> throw new IllegalArgumentException("Unexpected value: " + DbType);
            };

            return DriverManager.getConnection(url, DbUser, DbPassword);
        } else if ("LOCAL".equals(DB_LOCATION)) {
            return LocalDatabase.getConnection();
        } else {
            throw new IllegalArgumentException("Unsupported database location: " + DB_LOCATION);
        }
    }

    /**
     * Fetch and update missing player nicknames in the database.
     * This method is called after all player statistics have been synchronized, to fetch all missing player nicknames.
     *
     * @param connection The connection to the database.
     */
    public static void fetchAndUpdateMissingPlayerNicks(Connection connection) {
        ExecutorService executor = Executors.newFixedThreadPool(ConfigUtils.config.getInt("sync-thread-count")); // Počet vlákien na paralelné spracovanie
        String fetchMissingNicksSQL = "SELECT id, player_uuid FROM uuid_map WHERE player_nick IS NULL";

        try (PreparedStatement fetchStmt = connection.prepareStatement(fetchMissingNicksSQL);
             ResultSet rs = fetchStmt.executeQuery()) {

            // Get number of missing player nicks
            rs.last();
            int missingNicks = rs.getRow();
            rs.beforeFirst();
            StatSyncTask.totalPlayers = missingNicks;

            // Iterate over all players with missing nicks
            while (rs.next()) {
                int playerId = rs.getInt("id");
                String playerUUID = rs.getString("player_uuid");

                // Fetch player nick from API in a separate thread
                executor.submit(() -> {
                    try {
                        String playerNick = fetchPlayerNickFromAPI(playerUUID);
                        if (playerNick != null) {
                            updatePlayerNickInDatabase(connection, playerId, playerNick);
                        }
                    } catch (Exception e) {
                        System.err.println("Error fetching/updating nick for UUID: " + playerUUID);
                        e.printStackTrace();
                    }
                });
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        executor.shutdown();

        try {
            executor.awaitTermination(5, TimeUnit.MINUTES); // Maximálny čas na dokončenie
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Nick fetching process interrupted:");
            e.printStackTrace();
        }
    }

    /**
     * Get or insert (new) the player ID for the given UUID.
     *
     * @param connection The connection to the database.
     * @param playerUUID The UUID of the player.
     * @return The player ID.
     * @throws SQLException If an SQL error occurs.
     */
    public static int getOrInsertPlayerId(Connection connection, UUID playerUUID) throws SQLException {
        String selectSQL = "SELECT id FROM uuid_map WHERE player_uuid = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSQL)) {
            selectStmt.setString(1, playerUUID.toString());
            try (ResultSet rs = selectStmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("id");
                }
            }
        }

        String insertSQL = "INSERT INTO uuid_map (player_uuid) VALUES (?)";
        try (PreparedStatement insertStmt = connection.prepareStatement(insertSQL, PreparedStatement.RETURN_GENERATED_KEYS)) {
            insertStmt.setString(1, playerUUID.toString());
            insertStmt.executeUpdate();

            try (ResultSet rs = insertStmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }

        throw new SQLException("Failed to insert or retrieve player ID for UUID: " + playerUUID);
    }

    /**
     * Get the last global synchronization time from the database.
     * This method is used to determine which player statistics need to be synchronized.
     *
     * @param connection The connection to the database.
     */
    public static Timestamp getLastSyncTime(Connection connection) {
        String sql = "SELECT last_update FROM sync_metadata";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    // Try to fetch the last update time as a Timestamp
                    try {
                        return resultSet.getTimestamp("last_update");
                    } catch (Exception e) {
                        // If the timestamp is null or invalid, try to fetch it as a string
                        String lastUpdate = resultSet.getString("last_update");
                        if (lastUpdate != null) {
                            // Replace 'T' with ' ' for SQLite databases
                            if (lastUpdate.contains("T")) {
                                lastUpdate = lastUpdate.replace("T", " ");
                            }
                            return Timestamp.valueOf(lastUpdate); // Convert to Timestamp
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error fetching last sync time:");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Update the last global synchronization time in the database.
     * This method is called after all player statistics have been synchronized.
     *
     * @param connection The connection to the database.
     */
    public static void updateLastSyncTime(Connection connection) {
        String sql = "UPDATE sync_metadata SET last_update = ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            // Set the current timestamp as the last update time
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            String dbType = ConfigUtils.config.getString("database.type");
            if ("LOCAL".equals(DB_LOCATION)) {
                dbType = "SQLITE";
            }

            if ("SQLITE".equals(dbType)) {
                // For SQLite, we set the time as a string in ISO 8601 format
                String isoTime = now.toLocalDateTime().toString(); // Format: YYYY-MM-DDTHH:MM:SS
                statement.setString(1, isoTime);
            } else {
                // For MySQL/MariaDB/PostgreSQL set Timestamp directly
                statement.setTimestamp(1, now);
            }

            statement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Update the player nickname in the database.
     *
     * @param connection The connection to the database.
     * @param playerId The ID of the player.
     * @param playerNick The nickname of the player.
     * @throws SQLException If an SQL error occurs.
     */
    public static void updatePlayerNickInDatabase(Connection connection, int playerId, String playerNick) throws SQLException {
        String updateSQL = "UPDATE uuid_map SET player_nick = ? WHERE id = ?";
        try (PreparedStatement updateStmt = connection.prepareStatement(updateSQL)) {
            updateStmt.setString(1, playerNick);
            updateStmt.setInt(2, playerId);
            updateStmt.executeUpdate();

            // Increment synced players
            StatSyncTask.syncedPlayers++;
        }
    }

    /**
     * Synchronize player statistics with the database.
     * This method is called for each player file in a separate thread.
     *
     * @param connection The connection to the database.
     * @param playerUUID The UUID of the player.
     * @param stats The player statistics.
     */
    public static void syncPlayerStats(Connection connection, UUID playerUUID, JsonNode stats) {
        try {
            int playerId = getOrInsertPlayerId(connection, playerUUID);

            Map<String, List<String>> batchStatements = new HashMap<>();

            // Create batch statements for each stat type
            for (Iterator<String> it = stats.fieldNames(); it.hasNext(); ) {
                String statType = it.next();
                JsonNode statDetails = stats.get(statType);
                String tableName = "`" + statType.replace("minecraft:", "") + "`";
                System.out.println("Table name: " + tableName);
                List<String> statements = batchStatements.computeIfAbsent(tableName, k -> new ArrayList<>());

                statDetails.fields().forEachRemaining(entry -> {
                    String statName = entry.getKey();
                    int amount = entry.getValue().asInt();
                    statements.add(String.format("(%d, '%s', %d)", playerId, statName.replace("minecraft:", ""), amount));
                });
            }

            // Syntax differences for different database types
            String dbType = ConfigUtils.config.getString("database.type");
            if ("LOCAL".equals(DB_LOCATION)) {
                dbType = "SQLITE";
            }

            String finalDbType = dbType;
            batchStatements.forEach((tableName, statements) -> {
                String sql = switch (finalDbType) {
                    case "MARIADB", "MYSQL" ->
                            """
                                INSERT INTO %s (player_id, stat_name, amount)
                                VALUES %s
                                ON DUPLICATE KEY UPDATE amount = VALUES(amount)
                            """.formatted(tableName, String.join(", ", statements));
                    case "SQLITE" ->
                            """
                                INSERT INTO %s (player_id, stat_name, amount)
                                VALUES %s
                                ON CONFLICT(player_id, stat_name) DO UPDATE SET amount = excluded.amount
                            """.formatted(tableName, String.join(", ", statements));
                    case "POSTGRESQL" ->
                            """
                                INSERT INTO %s (player_id, stat_name, amount)
                                VALUES %s
                                ON CONFLICT (player_id, stat_name) DO UPDATE SET amount = excluded.amount
                            """.formatted(tableName, String.join(", ", statements));
                    case null, default -> throw new IllegalArgumentException("Unsupported database type: " + finalDbType);
                };

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            // Increment synced players
            StatSyncTask.syncedPlayers++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Fetch the player nickname from the Minetools API.
     *
     * @param playerUUID The UUID of the player.
     * @return The player nickname or null if not found (or an error occurred).
     */
    private static String fetchPlayerNickFromAPI(String playerUUID) {
        String apiUrl = "https://api.minetools.eu/profile/" + playerUUID;
        try {
            // Create a connection to the API as a GET request
            URI uri = new URI(apiUrl);
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setRequestMethod("GET");

            // Read the response from the API
            try (Scanner scanner = new Scanner(connection.getInputStream())) {
                String response = scanner.useDelimiter("\\A").next();
                JsonNode rootNode = StatSyncTask.MAPPER.readTree(response);
                JsonNode decodedNode = rootNode.get("decoded");
                JsonNode profileNameNode = decodedNode.get("profileName");
                return profileNameNode != null ? profileNameNode.asText() : null;
            }
        } catch (Exception e) {
            System.err.println("Failed to fetch player nick for UUID: " + playerUUID);
            e.printStackTrace();
            return null;
        }
    }

    /**
     *   Update the positions for all players in the database.
     *   This method ranks players based on the amount of each stat and updates the position column in the database.
     *   Only the top 10 players for each stat are ranked.
     *
     *   @param connection The connection to the database.
     */
    public static void updatePositionsForTable(Connection connection) {
        for (String tableName : TABLE_NAMES) {
            try {
                // Reset the 'position' column in the table
                String resetPositionsSQL = "UPDATE " + tableName + " SET position = NULL";
                try (PreparedStatement resetStmt = connection.prepareStatement(resetPositionsSQL)) {
                    resetStmt.executeUpdate();
                }

                // Update the positions for top players in each stat
                if ("MYSQL".equalsIgnoreCase(DB_TYPE) || "MARIADB".equalsIgnoreCase(DB_TYPE)) {
                    updatePositionsMySQL(connection, tableName);
                } else if ("SQLITE".equalsIgnoreCase(DB_TYPE)) {
                    updatePositionsSQLite(connection, tableName);
                } else {
                    throw new UnsupportedOperationException("Unsupported database type: " + DB_TYPE);
                }
            } catch (SQLException e) {
                System.err.println("Error updating positions in table '" + tableName + "':");
                e.printStackTrace();
            }
        }
    }

    /**
     * Update the positions for all players in the database (MySQL and MariaDB version).
     * This method ranks players based on the amount of each stat and updates the position column in the database.
     *
     * @param connection The connection to the database.
     * @param tableName The name of the table.
     */
    private static void updatePositionsMySQL(Connection connection, String tableName) throws SQLException {
        // Create a temporary table for 'ranked_data'
        String createTempTableSQL = """
        CREATE TEMPORARY TABLE ranked_data AS
        SELECT
            player_id,
            stat_name,
            ROW_NUMBER() OVER (PARTITION BY stat_name ORDER BY amount DESC) AS rank
        FROM %s
    """.formatted(tableName);
        try (PreparedStatement createTempStmt = connection.prepareStatement(createTempTableSQL)) {
            createTempStmt.executeUpdate();
        }

        // Update the positions
        String updatePositionsSQL = """
        UPDATE %s
        JOIN ranked_data
        ON %s.player_id = ranked_data.player_id AND %s.stat_name = ranked_data.stat_name
        SET %s.position = ranked_data.rank
        WHERE ranked_data.rank <= 10
    """.formatted(tableName, tableName, tableName, tableName);
        try (PreparedStatement updateStmt = connection.prepareStatement(updatePositionsSQL)) {
            updateStmt.executeUpdate();
        }

        // Drop the temporary table
        String dropTempTableSQL = "DROP TEMPORARY TABLE ranked_data";
        try (PreparedStatement dropTempStmt = connection.prepareStatement(dropTempTableSQL)) {
            dropTempStmt.executeUpdate();
        }
    }

    /**
     * Update the positions for all players in the database (SQLite version).
     * This method ranks players based on the amount of each stat and updates the position column in the database.
     *
     * @param connection The connection to the database.
     * @param tableName The name of the table.
     */
    private static void updatePositionsSQLite(Connection connection, String tableName) throws SQLException {
        // Create a temporary table for 'ranked_data'
        String createTempTableSQL = """
        CREATE TEMPORARY TABLE ranked_data (
            player_id INT,
            stat_name VARCHAR(256),
            rank INT
        )
    """;
        try (PreparedStatement createTempStmt = connection.prepareStatement(createTempTableSQL)) {
            createTempStmt.executeUpdate();
        }

        // Fill the temporary table with ranks
        String fillTempTableSQL = """
        INSERT INTO ranked_data (player_id, stat_name, rank)
        SELECT
            player_id,
            stat_name,
            (SELECT COUNT(*) + 1
             FROM %s AS inner_table
             WHERE inner_table.stat_name = outer_table.stat_name
               AND inner_table.amount > outer_table.amount) AS rank
        FROM %s AS outer_table
        WHERE rank <= 10
    """.formatted(tableName, tableName);
        try (PreparedStatement fillTempStmt = connection.prepareStatement(fillTempTableSQL)) {
            fillTempStmt.executeUpdate();
        }

        // Update the positions
        String updatePositionsSQL = """
        UPDATE %s
        SET position = (
            SELECT rank
            FROM ranked_data
            WHERE ranked_data.player_id = %s.player_id
              AND ranked_data.stat_name = %s.stat_name
        )
        WHERE EXISTS (
            SELECT 1
            FROM ranked_data
            WHERE ranked_data.player_id = %s.player_id
              AND ranked_data.stat_name = %s.stat_name
        )
    """.formatted(tableName, tableName, tableName, tableName, tableName);
        try (PreparedStatement updateStmt = connection.prepareStatement(updatePositionsSQL)) {
            updateStmt.executeUpdate();
        }

        // Drop the temporary table
        String dropTempTableSQL = "DROP TEMPORARY TABLE ranked_data";
        try (PreparedStatement dropTempStmt = connection.prepareStatement(dropTempTableSQL)) {
            dropTempStmt.executeUpdate();
        }
    }

    /**
     * Populate the 'hall_of_fame' table with the top players based on their score.
     *
     * @param connection The connection to the database.
     */
    public static void populateHallOfFame(Connection connection) {
        try {
            // Reset the 'hall_of_fame' table
            String clearHallOfFameSQL = "TRUNCATE TABLE hall_of_fame";
            try (PreparedStatement clearStmt = connection.prepareStatement(clearHallOfFameSQL)) {
                clearStmt.executeUpdate();
            }

            // Calculate the score for each player
            String scoreCalculationSQL = generateScoreCalculationSQL();

            try (PreparedStatement scoreStmt = connection.prepareStatement(scoreCalculationSQL);
                 ResultSet rs = scoreStmt.executeQuery()) {

                // Insert the top players into the 'hall_of_fame' table
                String insertHallOfFameSQL = """
                INSERT INTO hall_of_fame (player_id, first_place, second_place, third_place, fourth_place, fifth_place, score)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

                try (PreparedStatement insertStmt = connection.prepareStatement(insertHallOfFameSQL)) {
                    while (rs.next()) {
                        insertStmt.setInt(1, rs.getInt("player_id"));
                        insertStmt.setInt(2, rs.getInt("first_place"));
                        insertStmt.setInt(3, rs.getInt("second_place"));
                        insertStmt.setInt(4, rs.getInt("third_place"));
                        insertStmt.setInt(5, rs.getInt("fourth_place"));
                        insertStmt.setInt(6, rs.getInt("fifth_place"));
                        insertStmt.setInt(7, rs.getInt("score"));
                        insertStmt.addBatch();
                    }
                    insertStmt.executeBatch();
                }
            }

        } catch (SQLException e) {
            System.err.println("Error populating hall_of_fame:");
            e.printStackTrace();
        }
    }

    /**
      * Generate the SQL query for calculating the score for each player.
      *
      * @return The SQL query for calculating the score.
      */
    private static String generateScoreCalculationSQL() {
        StringBuilder unionQueries = new StringBuilder();

        // Create a UNION query for all tables
        for (String tableName : TABLE_NAMES) {
            if (unionQueries.length() > 0) {
                unionQueries.append(" UNION ALL ");
            }
            unionQueries.append("""
            SELECT
                player_id,
                CASE WHEN position = 1 THEN 10 ELSE 0 END AS first_place,
                CASE WHEN position = 2 THEN 5 ELSE 0 END AS second_place,
                CASE WHEN position = 3 THEN 3 ELSE 0 END AS third_place,
                CASE WHEN position = 4 THEN 2 ELSE 0 END AS fourth_place,
                CASE WHEN position = 5 THEN 1 ELSE 0 END AS fifth_place
            FROM %s
            WHERE position BETWEEN 1 AND 5
        """.formatted(tableName));
        }

        // Calculate the total score for each player
        return """
        SELECT
            player_id,
            SUM(first_place) AS first_place,
            SUM(second_place) AS second_place,
            SUM(third_place) AS third_place,
            SUM(fourth_place) AS fourth_place,
            SUM(fifth_place) AS fifth_place,
            SUM(first_place + second_place + third_place + fourth_place + fifth_place) AS score
        FROM (
            %s
        ) AS all_stats
        GROUP BY player_id
        ORDER BY score DESC
    """.formatted(unionQueries.toString());
    }



}
