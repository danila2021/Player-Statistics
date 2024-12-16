package github.fnewell.playerstatistics.utils;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class StatSyncScheduler {

    // Scheduled executor service for automatic sync
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /*
    * Starts the scheduled sync task with the interval specified in the config and delays the first run by 1 minute
    * */
    public static void startScheduledSync() {
        int intervalMinutes = ConfigUtils.config.getInt("sync-interval");   // Get the interval from the config

        // If the interval is less than or equal to 0, disable the scheduled sync
        if (intervalMinutes <= 0) {
            return;
        }

        // Print info about the scheduled sync
        System.out.println("Player stats synchronization was successfully scheduled with an interval of " + intervalMinutes + " minutes.");

        // Schedule the sync task
        scheduler.scheduleAtFixedRate(() -> {
            if (Objects.equals(StatSyncTask.status, "Idle")) {
                try {
                    StatSyncTask.syncAllPlayerStats();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1, intervalMinutes, TimeUnit.MINUTES);
    }

    /*
    * Stops the scheduled sync task
    * */
    public static void stopScheduledSync() {
        System.out.println("Stopping scheduled synchronization ...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}
