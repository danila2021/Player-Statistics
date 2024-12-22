package github.fnewell.playerstatistics.db;

import github.fnewell.playerstatistics.PlayerStatistics;
import net.fabricmc.loader.api.FabricLoader;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.DriverManager;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;


public class DriverUtils {

    public static DriverShim customDriverShim;

    /**
      * Function to register a custom SQLite JDBC driver via DriverShim
      */
    public static void registerSQLite() {
        try {
            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Registering custom SQLite JDBC driver ..."); }

            // Path to the custom SQLite JDBC driver
            Path driverPath = FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics/libs/sqlite-jdbc-3.47.1.0.jar");
            URL driverUrl = driverPath.toUri().toURL();

            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Driver URL: {}", driverUrl); }

            // Load driver via custom ClassLoader
            URLClassLoader loader = new URLClassLoader(new URL[]{driverUrl}, ClassLoader.getPlatformClassLoader());
            Class<?> driverClass = Class.forName("org.sqlite.JDBC", true, loader);
            Driver customDriver = (Driver) driverClass.getDeclaredConstructor().newInstance();

            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Driver Class: {}; Loaded from: {}", customDriver.getClass().getName(), customDriver.getClass().getProtectionDomain().getCodeSource().getLocation()); }

            // Registration DriverShim instead of the original driver
            customDriverShim = new DriverShim(customDriver);
            DriverManager.registerDriver(customDriverShim);
            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Custom SQLite JDBC driver registered!"); }
        } catch (Exception e) {
            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Failed to register custom SQLite driver via DriverShim: {}", e.getMessage());
        }
    }

    /**
      * Entry point to check for required drivers and download them if missing.
      */
    public static void checkDrivers() {
        try {
            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Checking for required drivers ..."); }

            // Check and download missing drivers
            Map<String, String> drivers = Map.of(
                    "sqlite-jdbc-3.47.1.0.jar", "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.47.1.0/sqlite-jdbc-3.47.1.0.jar",
                    "mariadb-java-client-3.5.1.jar", "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.5.1/mariadb-java-client-3.5.1.jar",
                    "postgresql-42.7.4.jar", "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
            );

            ensureDriversExist(drivers, FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics/libs"));

            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Required drivers checked!"); }
        } catch (Exception e) {
            if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            PlayerStatistics.LOGGER.error("Failed to check required drivers: {}", e.getMessage());
        }
    }

    /**
     * Check if required driver JARs exist, and download them if missing.
     *
     * @param drivers A map where the key is the JAR file name and the value is the download URL.
     * @param driverFolder The folder where the drivers should be located.
     * @throws IOException If an error occurs while checking or downloading files.
     */
    public static void ensureDriversExist(Map<String, String> drivers, Path driverFolder) throws IOException, InterruptedException {
        if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Checking for required drivers (ensureDriversExist)..."); }

        // Ensure the folder exists
        if (!Files.exists(driverFolder)) {
            Files.createDirectories(driverFolder);
        }

        // Check and download missing drivers
        for (Map.Entry<String, String> entry : drivers.entrySet()) {
            String fileName = entry.getKey();
            String downloadUrl = entry.getValue();

            Path filePath = driverFolder.resolve(fileName);

            if (!Files.exists(filePath)) {
                downloadFile(downloadUrl, filePath);
            } else {
                if(PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Driver {} already exists.", fileName); }
            }
        }
    }

    /**
     * Download a file from a URL to a specific path.
     *
     * @param fileUrl The URL of the file to download.
     * @param destination The path where the file should be saved.
     * @throws IOException If an error occurs during the download.
     * @throws InterruptedException If the download is interrupted.
     */
    private static void downloadFile(String fileUrl, Path destination) throws IOException, InterruptedException {
        PlayerStatistics.LOGGER.info("Downloading {} to {}", fileUrl, destination);

        HttpResponse<Path> response;
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(fileUrl))
                    .GET()
                    .build();
            response = client.send(request, HttpResponse.BodyHandlers.ofFile(destination));
        }

        if (response.statusCode() != 200) {
            throw new IOException("Failed to download file: " + fileUrl + " (HTTP " + response.statusCode() + ")");
        }

        PlayerStatistics.LOGGER.info("... successfully downloaded");
    }
}
