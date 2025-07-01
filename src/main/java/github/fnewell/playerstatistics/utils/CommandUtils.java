package github.fnewell.playerstatistics.utils;

import com.mojang.brigadier.CommandDispatcher;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.network.chat.Component;
import net.minecraft.ChatFormatting;
import net.neoforged.neoforge.common.NeoForge;
import net.neoforged.neoforge.event.RegisterCommandsEvent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CommandUtils {

    // Executor for async tasks
    public static final ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Register commands
     */
    public static void registerCommands() {
        NeoForge.EVENT_BUS.addListener(CommandUtils::onRegisterCommands);
    }

    private static void onRegisterCommands(RegisterCommandsEvent event) {
        CommandDispatcher<CommandSourceStack> dispatcher = event.getDispatcher();

        // Register "/pstats help"
        dispatcher.register(Commands.literal("pstats")
                .requires(source -> source.hasPermission(2))
                .then(Commands.literal("help")
                        .executes(context -> {
                            context.getSource().sendSystemMessage(
                                Component.literal("-- Player Statistics Help --\n")
                                    .withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD)
                                    .append(Component.literal("/pstats help - Show this help\n")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal("/pstats sync - Synchronize all player statistics\n")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal("/pstats status - Show current synchronization status\n")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal("------------------------")
                                        .withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD))
                            );
                            return 1;
                        })
                )
        );

        // Register "/pstats sync"
        dispatcher.register(Commands.literal("pstats")
                .then(Commands.literal("sync")
                        .executes(context -> {
                            CommandSourceStack source = context.getSource();
                            source.sendSystemMessage(Component.literal("Player Statistics synchronization started ...")
                                .withStyle(ChatFormatting.GOLD));

                            // Run the synchronization task in a separate thread
                            executor.submit(() -> {
                                if (StatSyncTask.syncAllPlayerStats()) {
                                    source.sendSystemMessage(Component.literal("Player Statistics synchronization completed successfully!")
                                        .withStyle(ChatFormatting.GREEN));
                                } else {
                                    source.sendSystemMessage(Component.literal("Player Statistics synchronization failed!")
                                        .withStyle(ChatFormatting.RED));
                                }
                            });
                            return 1;
                        })
                )
        );

        // Register "/pstats status"
        dispatcher.register(Commands.literal("pstats")
                .then(Commands.literal("status")
                        .executes(context -> {
                            context.getSource().sendSystemMessage(
                                Component.literal("-- Player Statistics Status --\n")
                                    .withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD)
                                    .append(Component.literal("Status: ")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal(StatSyncTask.status + "\n")
                                        .withStyle(ChatFormatting.RED, ChatFormatting.BOLD))
                                    .append(Component.literal("Last sync: ")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal(StatSyncTask.lastSync + "\n")
                                        .withStyle(ChatFormatting.RED, ChatFormatting.BOLD))
                                    .append(Component.literal("Progress: ")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal(String.valueOf(StatSyncTask.progressFrom))
                                        .withStyle(ChatFormatting.RED, ChatFormatting.BOLD))
                                    .append(Component.literal("/")
                                        .withStyle(ChatFormatting.GOLD))
                                    .append(Component.literal(StatSyncTask.progressTo + "\n")
                                        .withStyle(ChatFormatting.RED, ChatFormatting.BOLD))
                                    .append(Component.literal("--------------------------")
                                        .withStyle(ChatFormatting.GOLD, ChatFormatting.BOLD))
                            );
                            return 1;
                        })
                )
        );
    }
}