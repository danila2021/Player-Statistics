package github.fnewell.playerstatistics.utils;

import com.mojang.logging.LogUtils;
import com.typesafe.config.*;
import github.fnewell.playerstatistics.PlayerStatistics;
import net.fabricmc.loader.api.FabricLoader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;


public class ConfigUtils {

    private static final Path configDir = FabricLoader.getInstance().getConfigDir();    // Path to the config directory
    private static final String CONFIG_FILE_NAME = "player-statistics.conf";            // Name of the config file

    // Static variable to hold the loaded config
    public static Config config;


    /**
      * This method is used to create the config folder and default files if they don't exist
      * @return boolean - true if the config was successfully initialized, otherwise false
      */
    public static boolean initializeConfig() {
        Path modConfigDir = configDir.resolve(PlayerStatistics.MOD_ID);

        // Check if the mod's config folder exists, create it if it doesn't
        if (!Files.exists(modConfigDir)) {
            try {
                Files.createDirectories(modConfigDir);
            } catch (IOException e) {
                PlayerStatistics.LOGGER.error("Failed to create config directory, with error: {}", e.getMessage());
                return false;
            }
        }

        // Check if mod's config exists, create it if it doesn't
        if (Files.notExists(modConfigDir.resolve(CONFIG_FILE_NAME)) && !copyDefaultConfig(modConfigDir)) {
            return false;
        }

        // Check if the mod's data folder exists, create it if it doesn't
        Path modDataDir = FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics");

        if (!Files.exists(modDataDir)) {
            try {
                Files.createDirectories(modDataDir);
            } catch (IOException e) {
                PlayerStatistics.LOGGER.error("Failed to create data directory, with error: {}", e.getMessage());
                return false;
            }
        }

        // Load the config and store it in the static variable
        config = loadConfig();

        return config != null;
    }

    /**
      * Copy the default configuration file from resources to the mod's config directory
      * @param modConfigDir - Path to the mod's config directory
      * @return boolean - true if the file was successfully copied, otherwise false
      */
    private static boolean copyDefaultConfig(Path modConfigDir) {
        Path configFile = modConfigDir.resolve(CONFIG_FILE_NAME);   // Path to the config file

        // Only copy the file if it doesn't already exist
        if (!Files.exists(configFile)) {
            try (InputStream in = ConfigUtils.class.getResourceAsStream("/default_player-statistics.conf")) {
                if (in == null) return false;
                Files.copy(in, configFile);
            } catch (IOException e) {
                PlayerStatistics.LOGGER.error("Failed to copy default config file, with error: {}", e.getMessage());
                return false;
            }
        }

        return true;
    }

    /**
      * Load the config file and parse it using Typesafe Config (HOCON)
      * @return Config - the parsed config file or null if an error occurred
      */
    private static Config loadConfig() {
        // Path to the config file
        Path configFile = configDir.resolve(PlayerStatistics.MOD_ID).resolve(CONFIG_FILE_NAME);

        // Check if the config file exists
        if (Files.exists(configFile)) {
            try {
                // Load the config file using Typesafe Config (HOCON)
                Config conf_file = ConfigFactory.parseFile(configFile.toFile(), ConfigParseOptions.defaults().setAllowMissing(true));

                // Check if the config file is not empty
                if (conf_file.isEmpty()) {
                    PlayerStatistics.LOGGER.warn("Config file is empty!");
                    return null;
                }

                // Check if the config file has the correct keys and values
                if (!conf_file.hasPath("sync-thread-count") || !conf_file.hasPath("sync-interval") || !conf_file.hasPath("web-server-section") || !conf_file.hasPath("database-section")) {
                    PlayerStatistics.LOGGER.warn("Config file is missing required keys!");
                    return null;
                }

                int sync_thread_count = conf_file.getInt("sync-thread-count");  // Get number of threads to use for synchronization
                int cpu_count = Runtime.getRuntime().availableProcessors();          // Get the number of available processors

                // If the sync-thread-count is 0, set it to the number of available processors
                if (sync_thread_count == 0) {
                    sync_thread_count = cpu_count;
                }
                // If the sync-thread-count is less than 0, set it to (available processors - value)
                else if (sync_thread_count < 0) {
                    sync_thread_count = cpu_count - sync_thread_count;
                    // If the value is less than 1, set it to 1
                    if (sync_thread_count < 1) {
                        sync_thread_count = 1;
                    }
                }
                // If the sync-thread-count is greater than the number of available processors, set it to the number of available processors
                else if (sync_thread_count > cpu_count) {
                    sync_thread_count = cpu_count;
                }

                // Check if the sync-interval is a valid number
                int sync_interval = conf_file.getInt("sync-interval");


                // Check if stats-folder is set up correctly
                String stats_folder;
                try {
                    stats_folder = conf_file.getString("stats-folder");

                    // Check if stats_folder is empty string or null, then set it to "stats"
                    if (stats_folder == null || stats_folder.isEmpty()) {
                        stats_folder = "world/stats";
                    } else {
                        // Check if there's a leading or trailing slash, remove it
                        if (stats_folder.startsWith("/") || stats_folder.endsWith("/")) {
                            stats_folder = stats_folder.replaceAll("^/|/$", "");
                        }
                    }

                } catch (ConfigException.Missing ignored) {
                    stats_folder = "world/stats";
                }


                // Check if the database section is set up correctly
                Config database = conf_file.getConfig("database-section");
                if (!database.hasPath("location")) {
                    LogUtils.getLogger().warn("Database location is missing!");
                    return null;
                }

                // Check if the location is set to REMOTE or LOCAL
                String db_location = database.getString("location");
                if (!db_location.equals("REMOTE") && !db_location.equals("LOCAL")) {
                    LogUtils.getLogger().warn("Database location is invalid!");
                    return null;
                }

                // Check web-server section
                Config webServer = conf_file.getConfig("web-server-section");
                if (webServer.getBoolean("enabled")) {
                    // Check if the web server port is a valid number
                    int web_server_port = webServer.getInt("port");

                    // Check if database is set to LOCAL
                    if (!database.getString("location").equals("LOCAL")) {
                        db_location = "LOCAL";
                        database = database.withValue("location", ConfigValueFactory.fromAnyRef("LOCAL"));
                    }
                }

                // If location is REMOTE, check if the other keys are set up correctly
                if (db_location.equals("REMOTE")) {
                    if (!database.hasPath("type") || !database.hasPath("name") || !database.hasPath("host") || !database.hasPath("port") || !database.hasPath("username") || !database.hasPath("password")) {
                        LogUtils.getLogger().warn("Remote database is missing required keys!");
                        return null;
                    }
                }

                // Set DB Type to SQLITE if location is LOCAL, otherwise use the value from the config
                if (db_location.equals("LOCAL")) {
                    database = database.withValue("type", ConfigValueFactory.fromAnyRef("SQLITE"));
                }

                // Check debug mode
                try {
                    PlayerStatistics.DEBUG = conf_file.getBoolean("debug");
                } catch (ConfigException.Missing ignored) {}

                // Return parsed values if everything is set up correctly
                return ConfigFactory.empty()
                        .withValue("sync-thread-count", ConfigValueFactory.fromAnyRef(sync_thread_count))
                        .withValue("sync-interval", ConfigValueFactory.fromAnyRef(sync_interval))
                        .withValue("stats-folder", ConfigValueFactory.fromAnyRef(stats_folder))
                        .withValue("web-server", webServer.root())
                        .withValue("database", database.root());
            } catch (Exception e) {
                PlayerStatistics.LOGGER.error("Failed to load config file, with error: {}", e.getMessage());
                return null;
            }
        }
        return null;
    }
}