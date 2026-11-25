package ink.ziip.hammer.migrate;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.jar.JarFile;

/** The same engine is used by the pre-start launcher and both plugin commands. */
public final class Main {
  private Main() {}

  public static Path configuration() {
    return configuration(Path.of("/network/HammerMigrate/network.json"));
  }

  static Path configuration(Path shared) {
    String fallback = Files.isRegularFile(shared)
        ? shared.toString() : "plugins/HammerMigrate/config.json";
    return Path.of(System.getProperty("hammermigrate.config", fallback)).toAbsolutePath();
  }

  static Path queuePath(Path config) throws IOException {
    config = config.toAbsolutePath().normalize();
    var json = com.google.gson.JsonParser.parseString(Files.readString(config)).getAsJsonObject();
    boolean explicitState = json.has("stateDirectory");
    Path root = Path.of(json.get(explicitState ? "stateDirectory" : "networkRoot").getAsString());
    if (!root.isAbsolute()) root = config.getParent().resolve(root);
    return root.resolve(explicitState ? "queue.json" : "migrate/HammerMigrate/queue.json").normalize();
  }

  public static Path jar() throws Exception {
    return Path.of(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }

  public static synchronized Path extract(Path state) throws Exception {
    String digest;
    try (InputStream source = Files.newInputStream(jar())) {
      var hash = java.security.MessageDigest.getInstance("SHA-256");
      byte[] buffer = new byte[65536];
      for (int count; (count = source.read(buffer)) != -1; ) hash.update(buffer, 0, count);
      digest = HexFormat.of().formatHex(hash.digest()).substring(0, 20);
    }
    Path parent = state.resolve("engines");
    Path engine = parent.resolve(digest);
    Files.createDirectories(parent);
    try (var channel =
            java.nio.channels.FileChannel.open(
                parent.resolve("extract.lock"),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        var lock = channel.lock()) {
      if (Files.exists(engine.resolve(".complete"))) return engine;
      Path temporary = Files.createTempDirectory(parent, ".extract-");
      try (JarFile archive = new JarFile(jar().toFile())) {
        for (var entry : Collections.list(archive.entries())) {
          if (!entry.isDirectory() && entry.getName().startsWith("engine/")) {
            Path target = temporary.resolve(entry.getName().substring(7)).normalize();
            if (!target.startsWith(temporary)) throw new IOException("Invalid engine resource");
            Files.createDirectories(target.getParent());
            try (InputStream source = archive.getInputStream(entry)) {
              Files.copy(source, target);
            }
          }
        }
        Files.writeString(temporary.resolve(".complete"), digest);
        Files.move(temporary, engine, StandardCopyOption.ATOMIC_MOVE);
      } finally {
        if (Files.exists(temporary)) {
          try (var paths = Files.walk(temporary)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
          }
        }
      }
    }
    return engine;
  }

  public static int run(Path config, List<String> args, java.util.function.Consumer<String> output)
      throws Exception {
    config = config.toAbsolutePath().normalize();
    Path engine = extract(config.getParent());
    String python = python(config);
    List<String> cmd =
        new ArrayList<>(
            List.of(
                python,
                "-B",
                engine.resolve("worker.py").toString(),
                "--config",
                config.toString(),
                "--jar",
                jar().toString()));
    cmd.addAll(args);
    ProcessBuilder builder = new ProcessBuilder(cmd).redirectErrorStream(true);
    builder.environment().put("PYTHONPATH", engine.resolve("vendor").toString());
    builder.environment().put("PYTHONDONTWRITEBYTECODE", "1");
    Process process = builder.start();
    try (var reader = process.inputReader()) {
      for (String line; (line = reader.readLine()) != null; ) output.accept(line);
    }
    return process.waitFor();
  }

  private static String python(Path config) throws IOException {
    String python = "python3";
    if (Files.exists(config)) {
      var json = com.google.gson.JsonParser.parseString(Files.readString(config)).getAsJsonObject();
      if (json.has("python")) python = json.get("python").getAsString();
    }
    if (python.contains("/") && !Path.of(python).isAbsolute()) {
      return config.getParent().resolve(python).normalize().toString();
    }
    return python;
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 2 && args[0].equals("--extract")) {
      System.out.println(extract(Path.of(args[1]).toAbsolutePath().normalize()));
      return;
    }
    if (args.length == 2 && args[0].equals("--python")) {
      System.out.println(python(Path.of(args[1]).toAbsolutePath().normalize()));
      return;
    }
    if (args.length < 2 || !args[0].equals("--config")) {
      System.err.println(
          "Usage: java -jar HammerMigrate.jar --config /path/config.json init --root /servers");
      System.exit(2);
    }
    System.exit(
        run(Path.of(args[1]), Arrays.asList(args).subList(2, args.length), System.out::println));
  }
}
