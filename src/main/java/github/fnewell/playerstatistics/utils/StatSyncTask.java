package github.fnewell.playerstatistics.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import github.fnewell.playerstatistics.localdatabase.LocalDatabase;

import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class StatSyncTask {

    // Database location (LOCAL or REMOTE)
    private static final String DB_LOCATION = ConfigUtils.config.getString("database.location");

    // Jackson ObjectMapper for JSON parsing
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Variables to store synchronization status
    public static boolean isSyncing = false;
    public static int syncedPlayers = 0;
    public static int totalPlayers = 0;


    /*
    * Synchronize all player statistics with the database.
    * This method is called periodically by the scheduler or manually by a command.
    * */
    public static void syncAllPlayerStats() {
        try {
            try (Connection connection = getDatabaseConnection()){

                // Database initialization
                String DbType = ConfigUtils.config.getString("database.type");
                if ("LOCAL".equals(DB_LOCATION)) {
                    DbType = "SQLITE";
                }
                DatabaseInitializer.initializeDatabase(connection, DbType);

                // Check if the stats folder exists
                Path statsDir = Path.of("world/stats");
                if (!Files.exists(statsDir)) {
                    return;
                }

                // Get the last global sync time
                Timestamp lastGlobalSyncTime = getLastSyncTime(connection);
                if (lastGlobalSyncTime == null) {
                    lastGlobalSyncTime = new Timestamp(0);
                }

                // Prefetching all 'lastModified' values
                Map<UUID, Timestamp> fileTimestamps = new HashMap<>();
                Map<UUID, Path> playerFiles = new HashMap<>();
                for (Path statsFile : Files.newDirectoryStream(statsDir, "*.json")) {
                    UUID playerUUID = extractUUIDFromFile(statsFile);
                    if (playerUUID != null) {
                        Timestamp lastModified = new Timestamp(Files.getLastModifiedTime(statsFile).toMillis());
                        fileTimestamps.put(playerUUID, lastModified);
                        playerFiles.put(playerUUID, statsFile);
                    }
                }

                // Set total players
                totalPlayers = fileTimestamps.size();

                // Set syncing status
                isSyncing = true;

                // Parallel synchronization of player statistics
                ExecutorService executor = Executors.newFixedThreadPool(ConfigUtils.config.getInt("sync-thread-count"));
                Timestamp finalLastGlobalSyncTime = lastGlobalSyncTime;
                fileTimestamps.forEach((playerUUID, lastModified) -> {
                    if (lastModified.after(finalLastGlobalSyncTime)) {
                        executor.submit(() -> {
                            try {
                                JsonNode rootNode = MAPPER.readTree(playerFiles.get(playerUUID).toFile());
                                JsonNode stats = rootNode.get("stats");
                                if (stats != null) {
                                    syncPlayerStats(connection, playerUUID, stats);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                });

                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);

                // Update the last sync time
                updateLastSyncTime(connection);

                // Set syncing status
                isSyncing = false;

                // Reset synced and total players
                totalPlayers = 0;
                syncedPlayers = 0;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /*
    * Get a connection to the database based on the configuration.
    * This method is used to connect to a local or remote database.
    * */
    public static Connection getDatabaseConnection() throws Exception {
        // Connect to the database based on the configuration
        if("REMOTE".equals(DB_LOCATION)) {
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

    /*
    * Synchronize player statistics with the database.
    * This method is called for each player file in a separate thread.
    * */
    private static void syncPlayerStats(Connection connection, UUID playerUUID, JsonNode stats) {
        Map<String, List<String>> batchStatements = new HashMap<>();

        // Create batch statements for each stat type
        for (Iterator<String> it = stats.fieldNames(); it.hasNext(); ) {
            String statType = it.next();
            JsonNode statDetails = stats.get(statType);
            String tableName = "`" + statType + "`";
            List<String> statements = batchStatements.computeIfAbsent(tableName, k -> new ArrayList<>());

            statDetails.fields().forEachRemaining(entry -> {
                String statName = entry.getKey();
                int amount = entry.getValue().asInt();
                statements.add(String.format("('%s', '%s', %d)", playerUUID.toString(), statName, amount));
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
                            INSERT INTO %s (player_uuid, stat_name, amount)
                            VALUES %s
                            ON DUPLICATE KEY UPDATE amount = amount + VALUES(amount)
                        """.formatted(tableName, String.join(", ", statements));
                case "SQLITE" ->
                        """
                            INSERT INTO %s (player_uuid, stat_name, amount)
                            VALUES %s
                            ON CONFLICT(player_uuid, stat_name) DO UPDATE SET amount = amount + excluded.amount
                        """.formatted(tableName, String.join(", ", statements));
                case "POSTGRESQL" ->
                        """
                            INSERT INTO %s (player_uuid, stat_name, amount)
                            VALUES %s
                            ON CONFLICT (player_uuid, stat_name) DO UPDATE SET amount = %s.amount + excluded.amount
                        """.formatted(tableName, String.join(", ", statements), tableName);
                case null, default -> throw new IllegalArgumentException("Unsupported database type: " + finalDbType);
            };

            // Execute the batch statement
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Increment synced players
        syncedPlayers++;
    }

    /*
    * Get the last global synchronization time from the database.
    * This method is used to determine which player statistics need to be synchronized.
    * */
    private static Timestamp getLastSyncTime(Connection connection) {
        String sql = "SELECT last_update FROM sync_metadata";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            try (var resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    // Skús načítať priamo ako Timestamp
                    try {
                        return resultSet.getTimestamp("last_update");
                    } catch (Exception e) {
                        // Ak Timestamp zlyhá, skús načítať ako String
                        String lastUpdate = resultSet.getString("last_update");
                        if (lastUpdate != null) {
                            // Ošetri formát pre SQLite
                            if (lastUpdate.contains("T")) {
                                lastUpdate = lastUpdate.replace("T", " ");
                            }
                            return Timestamp.valueOf(lastUpdate); // Konvertuj na Timestamp
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Chyba pri načítaní času poslednej synchronizácie:");
            e.printStackTrace();
        }
        return null;
    }

    /*
    * Update the last global synchronization time in the database.
    * This method is called after all player statistics have been synchronized.
    * */
    private static void updateLastSyncTime(Connection connection) {
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

    /*
    * Extract the UUID from a player statistics file.
    * */
    private static UUID extractUUIDFromFile(Path statsFile) {
        try {
            String fileName = statsFile.getFileName().toString();
            if (fileName.endsWith(".json")) {
                String uuidPart = fileName.substring(0, fileName.length() - 5); // Remove .json
                return UUID.fromString(uuidPart);
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return null;
    }
}
