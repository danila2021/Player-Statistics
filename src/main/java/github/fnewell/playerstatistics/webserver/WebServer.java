package github.fnewell.playerstatistics.webserver;

import github.fnewell.playerstatistics.utils.ConfigUtils;
import io.javalin.Javalin;
import github.fnewell.playerstatistics.utils.DatabaseUtils;

import java.util.List;
import java.util.Map;

public class WebServer {

    public static void startServer() {
        Javalin app = Javalin.create(config -> {
            config.staticFiles.add("/webpage");
        }).start(ConfigUtils.config.getInt("web-server.port"));

        // Search endpoint
        app.get("/search", ctx -> {
            String query = ctx.queryParam("query");
            List<Map<String, Object>> results = DatabaseUtils.searchPlayerStats(query);
            ctx.json(results);
        });
    }
}
