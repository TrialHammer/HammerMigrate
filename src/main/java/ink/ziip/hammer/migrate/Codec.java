package ink.ziip.hammer.migrate;

import java.io.*;
import net.jpountz.lz4.*;

public final class Codec {
  private Codec() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 1) throw new IllegalArgumentException("encode|decode");
    if (args[0].equals("decode")) {
      try (InputStream input = new LZ4BlockInputStream(System.in)) {
        input.transferTo(System.out);
      }
    } else if (args[0].equals("encode")) {
      try (OutputStream output = new LZ4BlockOutputStream(System.out)) {
        System.in.transferTo(output);
      }
    } else throw new IllegalArgumentException("encode|decode");
  }
}
