package github.fnewell.playerstatistics.utils;

import net.minecraft.server.command.ServerCommandSource;
import net.minecraft.text.Text;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback.*;
import static net.minecraft.server.command.CommandManager.literal;


public class CommandUtils {

    // Executor for async tasks
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();

    /*
    * Register commands
    * */
    public static void registerCommands() {

        // Register /statsync help
        EVENT.register((dispatcher, registryAccess, environment) -> dispatcher.register(literal("statsync")
                .then(literal("help")
                        .executes(context -> {
                            context.getSource().sendFeedback(() -> Text.of("Help command"), false);
                            return 1;
                        })
                )
        ));

        // Register /statsync sync
        EVENT.register((dispatcher, registryAccess, environment) -> dispatcher.register(literal("statsync")
                .then(literal("sync")
                        .executes(context -> {
                            ServerCommandSource source = context.getSource();
                            source.sendFeedback(() -> Text.of("Running sync on background..."), false);

                            // Spusti synchronizáciu na pozadí
                            executor.submit(() -> {
                                try {
                                    StatSyncTask.syncAllPlayerStats();
                                    source.sendFeedback(() -> Text.of("Synchronization successful!"), false);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    source.sendFeedback(() -> Text.of("Synchronization failed!"), false);
                                }
                            });

                            return 1;
                        })
                )
        ));

        // Register /statsync status
        EVENT.register((dispatcher, registryAccess, environment) -> dispatcher.register(literal("statsync")
                .then(literal("status")
                        .executes(context -> {
                            context.getSource().sendFeedback(() -> Text.of("Current status:"), false);
                            context.getSource().sendFeedback(() -> Text.of("Status: " + (StatSyncTask.isSyncing ? "Syncing data" : (StatSyncTask.isFetchingNicks ? "Fetching nicks" : "Idle"))), false);
                            if (StatSyncTask.isSyncing) {
                                context.getSource().sendFeedback(() -> Text.of("Synced players: " + StatSyncTask.syncedPlayers + "/" + StatSyncTask.totalPlayers), false);
                            }
                            if (StatSyncTask.isFetchingNicks) {
                                context.getSource().sendFeedback(() -> Text.of("Fetching nicks: " + StatSyncTask.syncedPlayers + "/" + StatSyncTask.totalPlayers), false);
                            }
                            return 1;
                        })
                )
        ));
    }
}