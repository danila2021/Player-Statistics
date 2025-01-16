package github.fnewell.playerstatistics;

import github.fnewell.playerstatistics.db.DriverUtils;
import github.fnewell.playerstatistics.utils.CommandUtils;
import github.fnewell.playerstatistics.utils.ConfigUtils;
import github.fnewell.playerstatistics.utils.StatSyncScheduler;
import github.fnewell.playerstatistics.webserver.WebServer;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;


public class PlayerStatistics implements ModInitializer {
	public static final String MOD_ID = "player-statistics";
	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);
	public static boolean DEBUG = false;

	private static boolean cleanedUp = false;							// Variable to store cleanup status
	public static List<ExecutorService> executors = new ArrayList<>();  // List of executors for async tasks

	@Override
	public void onInitialize() {
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

		// Run planned synchronization task
		ServerLifecycleEvents.SERVER_STARTED.register(server -> StatSyncScheduler.startScheduledSync());

		// Run web server if enabled in the config
		if(ConfigUtils.config.getBoolean("web-server.enabled")) {
			if (DEBUG) { LOGGER.info("Starting web server ..."); }
			WebServer.startServer();
			if (DEBUG) { LOGGER.info("Web server started!"); }
		}

		// Stop the scheduled synchronization task on server shutdown
		ServerLifecycleEvents.SERVER_STOPPING.register(server -> cleanup());
		ServerLifecycleEvents.SERVER_STOPPED.register(server -> cleanup());

		LOGGER.info("Player Statistics successfully loaded!");
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