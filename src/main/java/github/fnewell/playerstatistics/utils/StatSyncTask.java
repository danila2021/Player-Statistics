package github.fnewell.playerstatistics.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import github.fnewell.playerstatistics.PlayerStatistics;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
    public static String status = "Idle";       // Actual status of the synchronization
    public static String lastSync = "Never";    // Last synchronization time
    public static int progressFrom = 0;         // Number of already done tasks
    public static int progressTo = 0;           // Total number of tasks

    /**
      * Synchronize all player statistics with the database.
      * This method is called periodically by the scheduler or manually by a command.
      *
      * @return True if the synchronization was successful, false otherwise.
      */
    public static boolean syncAllPlayerStats() {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Synchronizing player stats ..."); }

        /*//////////  DEBUG
        DriverManager.getDrivers().asIterator().forEachRemaining(driver -> {
            System.out.println("Driver: " + driver.getClass().getName() + " loaded from " +
                    driver.getClass().getProtectionDomain().getCodeSource().getLocation());
        });

        String databasePath = String.valueOf(FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics/player-statistics.db"));
        String url = "jdbc:sqlite:" + databasePath;

        try {
            Properties props = new Properties();
            Connection connection = customDriverShim.connect(url, props);

            System.out.println("Connection: " + connection); // Connection: org.sqlite.jdbc4.JDBC4Connection@2ef48ada
            // Používaj spojenie
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Failed to connect using custom DriverShim: " + e.getMessage());
        }

        if (PlayerStatistics.DEBUG) { return false; }
        //////////  DEBUG*/

        try {
            try (Connection connection = getDatabaseConnection()) {
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Database connection established."); }
                status = "Initializing";

                // Database initialization
                String DbType = ConfigUtils.config.getString("database.type");
                if ("LOCAL".equals(DatabaseUtils.DB_LOCATION)) {
                    DbType = "SQLITE";
                }
                DatabaseInitializer.initializeDatabase(connection, DbType);
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Database initialized."); }

                // Check if the stats folder exists
                Path statsDir = Path.of("world/stats");
                if (!Files.exists(statsDir)) {
                    PlayerStatistics.LOGGER.warn("Stats folder not found, skipping synchronization.");
                    status = "Idle";
                    return false;
                }
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Stats folder found."); }

                // Get the last global sync time
                Timestamp lastGlobalSyncTime = getLastSyncTime(connection);
                if (lastGlobalSyncTime == null) {
                    lastGlobalSyncTime = new Timestamp(0);
                }
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Last global sync time: {}", lastGlobalSyncTime); }

                // Prefetching all 'lastModified' values
                Map<UUID, Timestamp> fileTimestamps = new HashMap<>();
                Map<UUID, Path> playerFiles = new HashMap<>();
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(statsDir, "*.json")) {
                    for (Path statsFile : directoryStream) {
                        UUID playerUUID = extractUUIDFromFile(statsFile);
                        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("... Player UUID: {}", playerUUID); }
                        if (playerUUID != null) {
                            Timestamp lastModified = new Timestamp(Files.getLastModifiedTime(statsFile).toMillis());
                            fileTimestamps.put(playerUUID, lastModified);
                            playerFiles.put(playerUUID, statsFile);
                        }
                    }
                } catch (IOException e) {
                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
                    PlayerStatistics.LOGGER.error("Error while reading stats folder: {}", e.getMessage());
                    status = "Idle";
                    return false;
                }

                // Set total players
                progressTo = fileTimestamps.size();

                // Set syncing status
                status = "Syncing data";

                // Parallel synchronization of player statistics
                Timestamp finalLastGlobalSyncTime = lastGlobalSyncTime;

                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Syncing player stats ..."); }

                try (ExecutorService executor = Executors.newFixedThreadPool(ConfigUtils.config.getInt("sync-thread-count"))) {
                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Executor created."); }

                    fileTimestamps.forEach((playerUUID, lastModified) -> {
                        if (lastModified.after(finalLastGlobalSyncTime)) {
                            executor.submit(() -> {
                                try {
                                    JsonNode rootNode = MAPPER.readTree(playerFiles.get(playerUUID).toFile());
                                    JsonNode stats = rootNode.get("stats");

                                    if (stats != null) {
                                        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("/Syncing player stats for UUID: {} (executor: {})", playerUUID, Thread.currentThread().threadId()); }
                                        syncPlayerStats(connection, playerUUID, lastModified, stats);
                                        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("\\Player stats synced for UUID: {} (executor: {})", playerUUID, Thread.currentThread().threadId()); }
                                    }
                                } catch (Exception e) {
                                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
                                    PlayerStatistics.LOGGER.error("Error while syncing player stats: {}", e.getMessage());
                                }
                            });
                        }
                    });

                    executor.shutdown();
                    if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
                    PlayerStatistics.LOGGER.error("Executor interrupted: {}", e.getMessage());

                    status = "Idle";
                    return false;
                }


                // Reset synced counters
                progressTo = 0;
                progressFrom = 0;

                // Fetch and update missing player nicks
                status = "Fetching nicks";
                fetchAndUpdateMissingPlayerNicks(connection);
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Player nicks fetched."); }

                // Reset fetched counters
                progressTo = 0;
                progressFrom = 0;

                // Update the positions of the players in the database
                status = "Updating positions";
                DatabaseUtils.updatePositionsForTable(connection);
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Player positions updated."); }

                progressTo = 0;
                progressFrom = 0;

                // Populate Hall of Fame table with the top players
                status = "Populating Hall of Fame";
                DatabaseUtils.populateHallOfFame(connection);
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Hall of Fame populated."); }

                // Get server name, description, url and server icon
                // Server name and url from config file
                // Server description from server.properties
                // Server icon from server-icon.png
                String serverName = ConfigUtils.config.getString("web-server.server-name");
                String serverDesc = null;
                String serverUrl = ConfigUtils.config.getString("web-server.server-url");
                byte[] serverIcon = null;

                // Try to read server description from server.properties
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Reading server properties ..."); }
                File serverPropertiesFile = new File("server.properties");
                if (serverPropertiesFile.exists()) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(serverPropertiesFile), StandardCharsets.UTF_8))) {
                        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Server properties file found."); }
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.startsWith("motd=")) {
                                serverDesc = line.substring(5);
                                break;
                            }
                        }
                    } catch (IOException e) {
                        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
                        PlayerStatistics.LOGGER.error("Error while reading server.properties: {}", e.getMessage());

                        status = "Idle";
                        return false;
                    }
                }

                // Try to read server icon from server-icon.png
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Reading server icon ..."); }
                Path serverIconPath = Path.of("server-icon.png");
                if (Files.exists(serverIconPath)) {
                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Server icon found."); }
                    serverIcon = Files.readAllBytes(serverIconPath);
                }

                // Update Sync Metadata
                updateSyncMetadata(connection, serverName, serverDesc, serverUrl, serverIcon);
                if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Sync metadata updated."); }

                status = "Idle";
                return true;
            }
        } catch (Exception e) {
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Error while synchronizing player stats: {}", e.getMessage());
            status = "Idle";
            return false;
        }
    }

    /**
      * Extract the UUID from a player statistics file path.
      *
      * @param statsFile The path to the player statistics file.
      * @return The UUID of the player.
      */
    private static UUID extractUUIDFromFile(Path statsFile) {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Extracting UUID from file: {}", statsFile); }

        try {
            String fileName = statsFile.getFileName().toString();
            if (fileName.endsWith(".json")) {
                String uuidPart = fileName.substring(0, fileName.length() - 5); // Remove ".json"
                return UUID.fromString(uuidPart);
            }
        } catch (IllegalArgumentException e) {
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Error while extracting UUID from file: {}", e.getMessage());
        }
        return null;
    }
}
