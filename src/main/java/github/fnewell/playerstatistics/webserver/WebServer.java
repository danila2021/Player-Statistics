package github.fnewell.playerstatistics.webserver;

import github.fnewell.playerstatistics.PlayerStatistics;
import github.fnewell.playerstatistics.utils.ConfigUtils;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import net.fabricmc.loader.api.FabricLoader;

import java.nio.file.Files;
import java.nio.file.Path;

public class WebServer {

    // Javalin web server instance
    private static Javalin app;

    /**
     * Starts the web server
     * */
    public static void startServer() {
        int port = ConfigUtils.config.getInt("web-server.port");

        app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            config.staticFiles.add("/webpage", Location.CLASSPATH);
        }).start(port);

        PlayerStatistics.LOGGER.info("Web server running on port {}", port);

        // Endpoint to get the player-statistics.db file
        app.get("/player-statistics.db", ctx -> {
            Path dbPath = FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics/player-statistics.db");

            if (Files.exists(dbPath)) {
                ctx.contentType("application/octet-stream");
                ctx.result(Files.newInputStream(dbPath));
            } else {
                ctx.status(404).result("File not found");
            }
        });
    }

    /**
     * Stops the web server
     * */
    public static void stopServer() {
        if (app != null) {
            app.stop();
            PlayerStatistics.LOGGER.info("Web server stopped");
        }
    }
}