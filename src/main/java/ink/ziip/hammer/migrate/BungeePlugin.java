package ink.ziip.hammer.migrate;

import java.nio.file.*;
import java.util.*;
import net.md_5.bungee.api.ChatColor;
import net.md_5.bungee.api.CommandSender;
import net.md_5.bungee.api.chat.TextComponent;
import net.md_5.bungee.api.plugin.Command;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.api.plugin.TabExecutor;

public final class BungeePlugin extends Plugin {
  private static final List<String> SUBCOMMANDS =
      List.of("queue", "scan", "dry-run", "progress", "status", "rollback", "cancel", "retry");
  private Path config;

  @Override
  public void onLoad() {
    try {
      config = Main.configuration();
      if (!Files.exists(config)) {
        Files.createDirectories(config.getParent());
        try (var source = getResourceAsStream("config.json")) {
          Files.copy(Objects.requireNonNull(source), config);
        }
      }
      if (Main.run(config, List.of("startup", "--server", Path.of("").toAbsolutePath().normalize().toString()), getLogger()::info) != 0)
        throw new IllegalStateException("Pending migration failed");
    } catch (Exception e) {
      getLogger().severe("HammerMigrate blocked startup: " + e.getMessage());
      Runtime.getRuntime().halt(1);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void onEnable() {
    getProxy().getPluginManager().registerCommand(this, new MigrateCommand());
  }

  private final class MigrateCommand extends Command implements TabExecutor {
    MigrateCommand() { super("hmigrate", "hammermigrate.admin"); }

    @Override
    public void execute(CommandSender sender, String[] args) {
      if (args.length == 0 || !SUBCOMMANDS.contains(args[0].toLowerCase(Locale.ROOT))) {
        help(sender);
        return;
      }
      getProxy().getScheduler().runAsync(BungeePlugin.this, () -> {
        try {
          int result = Main.run(config, Arrays.asList(args), line -> sender.sendMessage(message(line)));
          if (result != 0) sender.sendMessage(message("[ERROR] 请求失败，请检查 manifest 和 worker 日志。"));
        } catch (Exception e) {
          sender.sendMessage(message("[ERROR] " + e.getMessage()));
        }
      });
    }

    @Override
    public Iterable<String> onTabComplete(CommandSender sender, String[] args) {
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
      if ((action.equals("progress") || Set.of("rollback", "cancel", "retry").contains(action)) && args.length == 2)
        return matching(jobIds(), args[1]);
      return List.of();
    }

    private List<String> jobIds() {
      try {
        Path queue = Main.queuePath(config);
        if (!Files.isRegularFile(queue)) return List.of();
        var matcher = java.util.regex.Pattern.compile("\\\"id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").matcher(Files.readString(queue));
        List<String> ids = new ArrayList<>();
        while (matcher.find()) ids.add(matcher.group(1));
        return ids;
      } catch (Exception ignored) { return List.of(); }
    }
  }

  private static List<String> matching(List<String> values, String prefix) {
    String lower = prefix.toLowerCase(Locale.ROOT);
    return values.stream().filter(value -> value.toLowerCase(Locale.ROOT).startsWith(lower)).toList();
  }

  private static void help(CommandSender sender) {
    sender.sendMessage(message("[OK] HammerMigrate · 离线玩家迁移"));
    sender.sendMessage(message("/hmigrate queue <旧名> <新名> [--premium on|off] [--merge]"));
    sender.sendMessage(message("/hmigrate scan|dry-run <旧名> <新名> [--premium on|off] [--fresh] [--merge]"));
    sender.sendMessage(message("/hmigrate status|progress [任务ID] · rollback|cancel|retry <任务ID>"));
  }

  private static TextComponent message(String line) {
    TextComponent result = new TextComponent("[HammerMigrate] " + line.replaceFirst("^\\[(ERROR|WARN|OK)]\\s*", ""));
    String lower = line.toLowerCase(Locale.ROOT);
    result.setColor(line.startsWith("[ERROR]") || lower.contains("error") || lower.contains("failed")
        ? ChatColor.RED
        : line.startsWith("[WARN]") || lower.contains("warning")
            ? ChatColor.YELLOW
            : line.startsWith("[OK]") ? ChatColor.GREEN : ChatColor.GRAY);
    return result;
  }
}
