package github.fnewell.playerstatistics.utils;

import github.fnewell.playerstatistics.PlayerStatistics;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class StatSyncScheduler {

    private static boolean isScheduled = false;  // Flag to check if the sync task is scheduled

    // Scheduled executor service for automatic sync
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
      * Starts the scheduled sync task with the interval specified in the config and delays the first run by 1 minute
      */
    public static void startScheduledSync() {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Starting scheduled synchronization ..."); }

        int intervalMinutes = ConfigUtils.config.getInt("sync-interval");   // Get the interval from the config

        // If the interval is less than or equal to 0, disable the scheduled sync
        if (intervalMinutes <= 0) {
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Scheduled synchronization is disabled."); }
            return;
        }

        // Schedule the sync task
        scheduler.scheduleAtFixedRate(() -> {
            if (Objects.equals(StatSyncTask.status, "Idle")) {
                try {
                    StatSyncTask.syncAllPlayerStats();
                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Scheduled synchronization task completed successfully."); }
                } catch (Exception e) {
                    if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Trace: ", e); }
                    PlayerStatistics.LOGGER.error("An error occurred while trying to schedule the synchronization task: {}", e.getMessage());
                }
            }
        }, 1, intervalMinutes, TimeUnit.MINUTES);
        isScheduled = true;

        // Print info about the scheduled sync
        PlayerStatistics.LOGGER.info("Player stats synchronization was successfully scheduled with an interval of {} minutes.", intervalMinutes);
    }

    /**
      * Stops the scheduled sync task
      */
    public static void stopScheduledSync() {
        if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Stopping scheduled synchronization ..."); }

        if (!isScheduled) {
            if (PlayerStatistics.DEBUG) { PlayerStatistics.LOGGER.info("Scheduled synchronization is not running."); }
            return;
        }

        PlayerStatistics.LOGGER.info("Stopping scheduled synchronization ...");
        scheduler.shutdown();

        try {
            if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }

        isScheduled = false;
    }
}
