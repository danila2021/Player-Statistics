package github.fnewell.playerstatistics.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static github.fnewell.playerstatistics.utils.DatabaseUtils.*;


public class StatSyncTask {
    // Jackson ObjectMapper for JSON parsing
    public static final ObjectMapper MAPPER = new ObjectMapper();

    // Variables to store synchronization status
    public static String status = "Idle";
    // public static boolean isSyncing = false;
    public static int syncedPlayers = 0;
    public static int totalPlayers = 0;
    // public static boolean isFetchingNicks = false;

    /**
     * Synchronize all player statistics with the database.
     * This method is called periodically by the scheduler or manually by a command.
     */
    public static void syncAllPlayerStats() {
        try {
            try (Connection connection = getDatabaseConnection()) {
                status = "Initializing";

                // Database initialization
                String DbType = ConfigUtils.config.getString("database.type");
                if ("LOCAL".equals(DatabaseUtils.DB_LOCATION)) {
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
                status = "Syncing data";

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

                // Reset synced and total players
                totalPlayers = 0;
                syncedPlayers = 0;

                // Fetch and update missing player nicks
                status = "Fetching nicks";
                fetchAndUpdateMissingPlayerNicks(connection);

                // Reset synced players
                totalPlayers = 0;
                syncedPlayers = 0;

                // Update the positions of the players in the database
                status = "Updating positions";
                DatabaseUtils.updatePositionsForTable(connection);

                // Populate Hall of Fame table with the top players
                status = "Populating Hall of Fame";
                DatabaseUtils.populateHallOfFame(connection);

                // Update the last sync time
                updateLastSyncTime(connection);

                status = "Idle";

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Extract the UUID from a player statistics file path.
     *
     * @param statsFile The path to the player statistics file.
     * @return The UUID of the player.
     */
    private static UUID extractUUIDFromFile(Path statsFile) {
        try {
            String fileName = statsFile.getFileName().toString();
            if (fileName.endsWith(".json")) {
                String uuidPart = fileName.substring(0, fileName.length() - 5); // Remove ".json"
                return UUID.fromString(uuidPart);
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return null;
    }
}
