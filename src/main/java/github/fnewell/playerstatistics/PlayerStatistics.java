package github.fnewell.playerstatistics;

import github.fnewell.playerstatistics.db.DriverUtils;
import github.fnewell.playerstatistics.utils.CommandUtils;
import github.fnewell.playerstatistics.utils.ConfigUtils;
import github.fnewell.playerstatistics.utils.StatSyncScheduler;
import github.fnewell.playerstatistics.webserver.WebServer;
import net.neoforged.bus.api.IEventBus;
import net.neoforged.fml.ModContainer;
import net.neoforged.fml.common.Mod;
import net.neoforged.fml.event.lifecycle.FMLCommonSetupEvent;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.server.ServerStartedEvent;
import net.neoforged.neoforge.event.server.ServerStoppedEvent;
import net.neoforged.neoforge.event.server.ServerStoppingEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

@Mod(PlayerStatistics.MOD_ID)
public class PlayerStatistics {
    public static final String MOD_ID = "player-statistics";
    public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);
    public static boolean DEBUG = false;

    private static boolean cleanedUp = false;
    public static List<ExecutorService> executors = new ArrayList<>();

    public PlayerStatistics(IEventBus modEventBus, ModContainer modContainer) {
        modEventBus.addListener(this::commonSetup);
        
        NeoForge.EVENT_BUS.addListener(this::onServerStarted);
        NeoForge.EVENT_BUS.addListener(this::onServerStopping);
        NeoForge.EVENT_BUS.addListener(this::onServerStopped);
    }

    private void commonSetup(final FMLCommonSetupEvent event) {
        LOGGER.info("Loading Player Statistics ...");

        // Initialize the config
        if(!ConfigUtils.initializeConfig()) {
            LOGGER.error("Failed to initialize config. Mod is not loaded!");
            return;
        }

        // DEBUG: Check if debug mode is enabled
        if (DEBUG) { LOGGER.info("Debug mode is enabled!"); }

        // Check for required drivers and download them if missing
        DriverUtils.checkDrivers();

        // Register the custom DB driver
        DriverUtils.registerSQLite();

        // Register commands
        if (DEBUG) { LOGGER.info("Registering commands ..."); }
        CommandUtils.registerCommands();
        if (DEBUG) { LOGGER.info("Commands registered!"); }

        // Run web server if enabled in the config
        if(ConfigUtils.config.getBoolean("web-server.enabled")) {
            if (DEBUG) { LOGGER.info("Starting web server ..."); }
            WebServer.startServer();
            if (DEBUG) { LOGGER.info("Web server started!"); }
        }

        LOGGER.info("Player Statistics successfully loaded!");
    }

    private void onServerStarted(ServerStartedEvent event) {
        StatSyncScheduler.startScheduledSync();
    }

    private void onServerStopping(ServerStoppingEvent event) {
        cleanup();
    }

    private void onServerStopped(ServerStoppedEvent event) {
        cleanup();
    }

    private static void cleanup() {
        if (DEBUG) { LOGGER.info("Cleaning up ..."); }

        if (cleanedUp) {
            if (DEBUG) { LOGGER.info("Cleanup already completed!"); }
            return;
        }

        // Stop all executors
        for (ExecutorService executor : executors) {
            if (DEBUG) { LOGGER.info("Shutting down executor ..."); }
            executor.shutdownNow();
        }

        // Stop CommandUtils executor
        if (DEBUG) { LOGGER.info("Shutting down CommandUtils executor ..."); }
        CommandUtils.executor.shutdownNow();

        // Stop the scheduled synchronization task
        if (DEBUG) { LOGGER.info("Stopping scheduled synchronization ..."); }
        StatSyncScheduler.stopScheduledSync();

        // Stop the web server
        if (DEBUG) { LOGGER.info("Stopping web server ..."); }
        WebServer.stopServer();

        cleanedUp = true;

        if (DEBUG) { LOGGER.info("Cleanup completed!"); }
    }
}