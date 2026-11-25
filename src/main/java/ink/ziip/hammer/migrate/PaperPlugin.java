package ink.ziip.hammer.migrate;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bukkit.ChatColor;
import org.bukkit.command.*;
import org.bukkit.plugin.java.JavaPlugin;

public final class PaperPlugin extends JavaPlugin implements CommandExecutor, TabCompleter {
  private static final List<String> SUBCOMMANDS =
      List.of("queue", "scan", "dry-run", "progress", "status", "rollback", "cancel", "retry");
  private final AtomicBoolean busy = new AtomicBoolean();
  private Path config;

  @Override
  public void onLoad() {
    try {
      config = Main.configuration();
      if (!Files.exists(config)) {
        Files.createDirectories(config.getParent());
        try (var source = getResource("config.json")) {
          Files.copy(Objects.requireNonNull(source), config);
        }
      }
      if (Main.run(
              config,
              List.of("startup", "--server", Path.of("").toAbsolutePath().normalize().toString()),
              getLogger()::info)
          != 0) {
        throw new IllegalStateException(
            "Pending migration failed. Inspect the manifest before restarting.");
      }
    } catch (Exception e) {
      getLogger().severe("HammerMigrate blocked startup: " + e.getMessage());
      // This is the cold-start gate, before worlds and player sessions load.
      // Shutdown hooks can deadlock waiting for this same initialization thread.
      Runtime.getRuntime().halt(1);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void onEnable() {
    PluginCommand command = Objects.requireNonNull(getCommand("hmigrate"));
    command.setExecutor(this);
    command.setTabCompleter(this);
  }

  @Override
  public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
    if (!sender.hasPermission("hammermigrate.admin")) return true;
    List<String> arguments = Arrays.asList(args);
    if (args.length == 0
        || !Set.of("queue", "scan", "dry-run", "progress", "status", "rollback", "cancel", "retry")
            .contains(args[0])) {
      help(sender);
      return true;
    }
    boolean exclusive = !args[0].equals("progress");
    if (exclusive && !busy.compareAndSet(false, true)) {
      sender.sendMessage(ChatColor.RED + "HammerMigrate " + ChatColor.GRAY + "迁移任务正在执行。");
      return true;
    }
    getServer()
        .getScheduler()
        .runTaskAsynchronously(
            this,
            () -> {
              List<String> messages = new ArrayList<>();
              try {
                if (Main.run(config, arguments, messages::add) != 0)
                  messages.add("[ERROR] 请求失败，请检查 manifest 和 worker 日志。");
              } catch (Exception e) {
                messages.add("[ERROR] " + e.getMessage());
              } finally {
                if (exclusive) busy.set(false);
              }
              getServer()
                  .getScheduler()
                  .runTask(this, () -> messages.forEach(line -> sender.sendMessage(format(line))));
            });
    return true;
  }

  @Override
  public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
    if (!sender.hasPermission("hammermigrate.admin")) return List.of();
    if (args.length == 1) return matching(SUBCOMMANDS, args[0]);
    String action = args[0].toLowerCase(Locale.ROOT);
    if (Set.of("queue", "scan", "dry-run").contains(action)) {
      int index = args.length - 1;
      if (index >= 4 && args[index - 1].equalsIgnoreCase("--premium"))
        return matching(List.of("on", "off"), args[index]);
      if (index >= 3) {
        List<String> options = action.equals("queue")
            ? List.of("--premium", "--merge")
            : List.of("--premium", "--fresh", "--merge");
        return matching(options, args[index]);
      }
    }
    if (action.equals("cancel") && args.length == 3)
      return matching(List.of("--force"), args[2]);
    if ((action.equals("progress") && args.length == 2)
        || Set.of("rollback", "cancel", "retry").contains(action) && args.length == 2)
      return matching(jobIds(), args[1]);
    return List.of();
  }

  private List<String> jobIds() {
    try {
      Path queue = queuePath();
      if (!Files.isRegularFile(queue)) return List.of();
      String json = Files.readString(queue);
      var matcher = java.util.regex.Pattern.compile("\\\"id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").matcher(json);
      List<String> ids = new ArrayList<>();
      while (matcher.find()) ids.add(matcher.group(1));
      return ids;
    } catch (Exception ignored) {
      return List.of();
    }
  }

  private Path queuePath() throws Exception {
    return Main.queuePath(config);
  }

  private static List<String> matching(List<String> values, String prefix) {
    String lower = prefix.toLowerCase(Locale.ROOT);
    return values.stream().filter(value -> value.toLowerCase(Locale.ROOT).startsWith(lower)).toList();
  }

  private static void help(CommandSender sender) {
    sender.sendMessage(ChatColor.GOLD + "HammerMigrate" + ChatColor.DARK_GRAY + " ── " + ChatColor.GRAY + "离线玩家迁移");
    sender.sendMessage(ChatColor.YELLOW + "/hmigrate queue <旧名> <新名> " + ChatColor.GRAY + "[--premium on|off] [--merge]");
    sender.sendMessage(ChatColor.YELLOW + "/hmigrate scan|dry-run <旧名> <新名> " + ChatColor.GRAY + "[--premium on|off] [--fresh] [--merge]");
    sender.sendMessage(ChatColor.YELLOW + "/hmigrate status|progress [任务ID]" + ChatColor.GRAY + " · 查看状态");
    sender.sendMessage(ChatColor.YELLOW + "/hmigrate rollback|cancel|retry <任务ID>");
  }

  private static String format(String line) {
    String lower = line.toLowerCase(Locale.ROOT);
    String color = line.startsWith("[ERROR]") || lower.contains("error") || lower.contains("failed")
        ? ChatColor.RED.toString()
        : line.startsWith("[WARN]") || lower.contains("warning")
            ? ChatColor.YELLOW.toString()
            : line.startsWith("[OK]") ? ChatColor.GREEN.toString() : ChatColor.GRAY.toString();
    return ChatColor.DARK_AQUA + "[HammerMigrate] " + color + line.replaceFirst("^\\[(ERROR|WARN|OK)]\\s*", "");
  }
}
