package github.fnewell.playerstatistics.utils;

import net.minecraft.server.command.ServerCommandSource;
import net.minecraft.text.Style;
import net.minecraft.text.Text;
import net.minecraft.util.Formatting;
import me.lucko.fabric.api.permissions.v0.Permissions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback.*;
import static net.minecraft.server.command.CommandManager.literal;


public class CommandUtils {

    // Executor for async tasks
    private static final ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Register commands
     */
    public static void registerCommands() {

        // Register "/pstats help"
        EVENT.register((dispatcher, registryAccess, environment) -> dispatcher.register(literal("pstats")
                .requires(Permissions.require("pstats", 2))
                .then(literal("help")
                        .executes(context -> {
                            context.getSource().sendFeedback(() ->
                                    Text.literal("-- Player Statistics Help --\n").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.GOLD))
                                        .append(Text.literal("/statsync help - Show this help\n").setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal("/statsync sync - Synchronize all player statistics\n").setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal("/statsync status - Show current synchronization status\n").setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal("------------------------").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.GOLD))), false);
                            return 1;
                        })
                )
        ));

        // Register "/pstats sync"
        EVENT.register((dispatcher, registryAccess, environment) -> dispatcher.register(literal("pstats")
                .then(literal("sync")
                        .executes(context -> {
                            ServerCommandSource source = context.getSource();
                            source.sendFeedback(() -> Text.literal("Player Statistics synchronization started ...").setStyle(Style.EMPTY.withColor(Formatting.GOLD)), false);

                            // Run the synchronization task in a separate thread
                            executor.submit(() -> {
                                if (StatSyncTask.syncAllPlayerStats()) {
                                    source.sendFeedback(() -> Text.literal("Player Statistics synchronization completed successfully!").setStyle(Style.EMPTY.withColor(Formatting.GREEN)), false);
                                } else {
                                    source.sendFeedback(() -> Text.literal("Player Statistics synchronization failed!").setStyle(Style.EMPTY.withColor(Formatting.RED)), false);
                                }
                            });
                            return 1;
                        })
                )
        ));

        // Register "/psats status"
        EVENT.register((dispatcher, registryAccess, environment) -> dispatcher.register(literal("pstats")
                .then(literal("status")
                        .executes(context -> {
                            context.getSource().sendFeedback(() ->
                                    Text.literal("-- Player Statistics Status --\n").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.GOLD))
                                        .append(Text.literal("Status: ").setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal(StatSyncTask.status + "\n").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.RED)))
                                        .append(Text.literal("Last sync: ").setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal(StatSyncTask.lastSync + "\n").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.RED)))
                                        .append(Text.literal("Progress: " ).setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal(String.valueOf(StatSyncTask.progressFrom)).setStyle(Style.EMPTY.withBold(true).withColor(Formatting.RED)))
                                        .append(Text.literal("/").setStyle(Style.EMPTY.withBold(false).withColor(Formatting.GOLD)))
                                        .append(Text.literal(StatSyncTask.progressTo + "\n").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.RED)))
                                        .append(Text.literal("--------------------------").setStyle(Style.EMPTY.withBold(true).withColor(Formatting.GOLD))), false);
                            return 1;
                        })
                )
        ));
    }
}