package github.fnewell.playerstatistics.localdatabase;

import net.fabricmc.loader.api.FabricLoader;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class LocalDatabase {

    // Path to the SQLite database
    private static final Path DB_PATH = FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics/player-statistics.db");

    /*
    * Function to get a connection to the local SQLite database
    * @return Connection to the SQLite database
    * */
    public static Connection getConnection() throws SQLException {
        File dbFile = new File(DB_PATH.toString());
        dbFile.getParentFile().mkdirs(); // Create the directory if it doesn't exist

        return DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
    }
}

