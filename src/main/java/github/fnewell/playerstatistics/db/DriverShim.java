package github.fnewell.playerstatistics.db;

import github.fnewell.playerstatistics.PlayerStatistics;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;


/**
  * Class to wrap an existing JDBC driver so new drivers can be dynamically loaded and (masked) registered via DriverShim.
  * This is useful so loaded drivers will not interfere with other drivers that are already loaded or will be loaded.
  * Source: <a href="https://www.kfu.com/~nsayer/Java/dyn-jdbc.html">Pick your JDBC driver at runtime</a>
  */
public class DriverShim implements Driver {
    private final Driver driver;

    // Constructor to wrap an existing driver
    public DriverShim(Driver driver) {
        this.driver = driver;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("DriverShim used driver: {} Loaded from: {}", driver.getClass().getName(), driver.getClass().getProtectionDomain().getCodeSource().getLocation()); }
        return driver.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return driver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() {
        throw new UnsupportedOperationException("getParentLogger is not supported.");
    }
}
