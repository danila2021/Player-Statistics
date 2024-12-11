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


    /*
    * This method is used to create the config folder and default files if they don't exist
    * No returns, but it initializes the config variable
    * */
    public static void initializeConfig() {
        Path modConfigDir = configDir.resolve(PlayerStatistics.MOD_ID);

        // Check if the mod's config folder exists, create it if it doesn't
        if (!Files.exists(modConfigDir)) {
            try {
                Files.createDirectories(modConfigDir);
                // Now create the default config file(s)
                copyDefaultConfig(modConfigDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Load the config and store it in the static variable
        config = loadConfig();
    }

    /*
    * Copy the default configuration file from resources to the mod's config directory
    * @param modConfigDir - Path to the mod's config directory
    * No returns, but it creates the default config file
    * */
    private static void copyDefaultConfig(Path modConfigDir) {
        Path configFile = modConfigDir.resolve(CONFIG_FILE_NAME);   // Path to the config file

        // Only copy the file if it doesn't already exist
        if (!Files.exists(configFile)) {
            try (InputStream in = ConfigUtils.class.getResourceAsStream("/default_player-statistics.conf")) {
                if (in == null) return;
                Files.copy(in, configFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*
    * Load the config file and parse it using Typesafe Config (HOCON)
    * @return Config - the parsed config file
    * */
    private static Config loadConfig() {
        // Path to the config file
        Path configFile = configDir.resolve(PlayerStatistics.MOD_ID).resolve(CONFIG_FILE_NAME);

        // Check if the config file exists
        if (Files.exists(configFile)) {
            try {
                // Load the config file using Typesafe Config (HOCON)
                Config conf_file = ConfigFactory.parseFile(configFile.toFile(), ConfigParseOptions.defaults().setAllowMissing(true));

                // Check if the config file is set up correctly
                if (conf_file.isEmpty()) {
                    LogUtils.getLogger().error("Config file is empty!");
                    return ConfigFactory.empty();
                }

                System.out.println(conf_file);

                // Check if the config file has the correct keys and values
                if (!conf_file.hasPath("sync-thread-count") || !conf_file.hasPath("sync-interval") || !conf_file.hasPath("database-section") || !conf_file.hasPath("web-server-section")) {
                    LogUtils.getLogger().error("Config file is missing required keys!");
                    return ConfigFactory.empty();
                }

                // Check if the sync-thread-count is a valid number
                // If the value is 0, set it to the number of available processors
                // If the value is less than 0, set it to (available processors - value)
                // If the value is greater than the number of available processors, set it to the number of available processors
                int sync_thread_count = conf_file.getInt("sync-thread-count");
                int cpu_count = Runtime.getRuntime().availableProcessors();
                if (sync_thread_count == 0) {
                    sync_thread_count = cpu_count;
                } else if (sync_thread_count < 0) {
                    sync_thread_count = cpu_count - sync_thread_count;
                    if (sync_thread_count < 1) {
                        sync_thread_count = 1;
                    }
                } else if (sync_thread_count > cpu_count) {
                    sync_thread_count = cpu_count;
                }

                // Check if the sync-interval is a valid number
                int sync_interval = conf_file.getInt("sync-interval");

                // Check if the database section is set up correctly
                // If location is REMOTE or LOCAL and if REMOTE, check if the other keys are set up correctly
                Config database = conf_file.getConfig("database-section");
                if (!database.hasPath("location")) {
                    LogUtils.getLogger().error("Database location is missing!");
                    return ConfigFactory.empty();
                }

                String location = database.getString("location");
                if (location.equals("REMOTE")) {
                    if (!database.hasPath("type") || !database.hasPath("name") || !database.hasPath("host") || !database.hasPath("port") || !database.hasPath("username") || !database.hasPath("password")) {
                        LogUtils.getLogger().error("Database location is set to REMOTE but other keys are missing!");
                        return ConfigFactory.empty();
                    }
                } else if (!location.equals("LOCAL")) {
                    LogUtils.getLogger().error("Database location is invalid!");
                    return ConfigFactory.empty();
                }

                // Check if web-server section is set up correctly
                Config webServer = conf_file.getConfig("web-server-section");
                if (webServer.getBoolean("enabled")) {
                    // Check if the web server port is a valid number
                    int web_server_port = webServer.getInt("port");
                }

                // Return parsed values if everything is set up correctly
                return ConfigFactory.empty()
                        .withValue("sync-thread-count", ConfigValueFactory.fromAnyRef(sync_thread_count))
                        .withValue("sync-interval", ConfigValueFactory.fromAnyRef(sync_interval))
                        .withValue("web-server", webServer.root())
                        .withValue("database", database.root());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ConfigFactory.empty();  // Return an empty config if the file doesn't exist
    }
}