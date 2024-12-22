package github.fnewell.playerstatistics.db;

import github.fnewell.playerstatistics.PlayerStatistics;
import net.fabricmc.loader.api.FabricLoader;

import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;

import static github.fnewell.playerstatistics.db.DriverUtils.customDriverShim;


public class LocalDatabase {

    // Path to the SQLite database
    private static final Path DB_PATH = FabricLoader.getInstance().getGameDir().resolve("mods/player-statistics/player-statistics.db");

    /**
      * Function to get a connection to the local SQLite database
      * @return Connection to the SQLite database
      * @throws SQLException if a database access error occurs
      */
    public static Connection getConnection() throws SQLException {
        try {
            Properties properties = new Properties();
            Connection connection = customDriverShim.connect("jdbc:sqlite:" + DB_PATH, properties);

            if (connection == null) {
                throw new SQLException("Failed to connect to the local SQLite database: Connection is null");
            }

            return connection;
        } catch (SQLException e) {
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
            throw new SQLException("Failed to connect to the local SQLite database: " + e.getMessage());
        }
    }


}