package github.fnewell.playerstatistics;

import com.mojang.brigadier.Command;
import github.fnewell.playerstatistics.localdatabase.LocalDatabase;
import github.fnewell.playerstatistics.utils.CommandUtils;
import github.fnewell.playerstatistics.utils.ConfigUtils;
import github.fnewell.playerstatistics.utils.StatSyncScheduler;
import github.fnewell.playerstatistics.webserver.WebServer;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlayerStatistics implements ModInitializer {
	public static final String MOD_ID = "player-statistics";

	// This logger is used to write text to the console and the log file.
	// It is considered best practice to use your mod id as the logger's name.
	// That way, it's clear which mod wrote info, warnings, and errors.
	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

	@Override
	public void onInitialize() {
		// Initialize the config
		ConfigUtils.initializeConfig();

		// Register commands
		CommandUtils.registerCommands();

		// Run planned synchronization task
		StatSyncScheduler.startScheduledSync();

		// Run web server if enabled in the config
		/*if(ConfigUtils.config.getBoolean("web-server.enabled"))
			WebServer.startServer();*/

		// Stop the scheduled synchronization task on server shutdown
		ServerLifecycleEvents.SERVER_STOPPING.register(server -> StatSyncScheduler.stopScheduledSync());

		LOGGER.info("PlayerStatistics loaded!");
	}
}