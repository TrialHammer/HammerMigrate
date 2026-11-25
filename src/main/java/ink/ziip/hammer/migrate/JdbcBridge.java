package ink.ziip.hammer.migrate;

import com.google.gson.*;
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Private JSON-lines RPC to an offline H2 or HyperSQL file, no credentials in argv. */
public final class JdbcBridge {
  private static final Gson JSON = new GsonBuilder().serializeNulls().create();

  private JdbcBridge() {}

  private static JsonElement value(Object value) throws SQLException {
    if (value == null) return JsonNull.INSTANCE;
    if (value instanceof Blob blob) value = blob.getBytes(1, Math.toIntExact(blob.length()));
    if (value instanceof Clob clob) value = clob.getSubString(1, Math.toIntExact(clob.length()));
    if (value instanceof byte[] bytes) {
      JsonObject out = new JsonObject();
      out.addProperty("$bytes", Base64.getEncoder().encodeToString(bytes));
      return out;
    }
    if (value instanceof BigDecimal decimal) {
      JsonObject out = new JsonObject();
      out.addProperty("$decimal", decimal.toPlainString());
      return out;
    }
    if (value instanceof Number number) return new JsonPrimitive(number);
    if (value instanceof Boolean bool) return new JsonPrimitive(bool);
    if (value instanceof UUID id) return new JsonPrimitive(id.toString());
    return new JsonPrimitive(value.toString());
  }

  private static Object parameter(JsonElement e) {
    if (e.isJsonNull()) return null;
    if (e.isJsonObject()) {
      if (e.getAsJsonObject().has("$bytes"))
        return Base64.getDecoder().decode(e.getAsJsonObject().get("$bytes").getAsString());
      if (e.getAsJsonObject().has("$decimal"))
        return new BigDecimal(e.getAsJsonObject().get("$decimal").getAsString());
      throw new IllegalArgumentException("Unknown typed parameter");
    }
    JsonPrimitive p = e.getAsJsonPrimitive();
    if (p.isBoolean()) return p.getAsBoolean();
    if (p.isNumber()) return new BigDecimal(p.getAsString());
    return p.getAsString();
  }

  public static void main(String[] args) throws Exception {
    try (var input = new BufferedReader(new InputStreamReader(System.in))) {
      JsonObject init = JsonParser.parseString(input.readLine()).getAsJsonObject();
      String url = init.get("url").getAsString();
      if (!url.startsWith("jdbc:h2:file:") && !url.startsWith("jdbc:hsqldb:file:"))
        throw new IllegalArgumentException("Only file JDBC databases allowed");
      try (Connection connection =
          DriverManager.getConnection(
              url, init.get("user").getAsString(), init.get("password").getAsString())) {
        connection.setAutoCommit(false);
        System.out.println("{\"ok\":true}");
        for (String line; (line = input.readLine()) != null; ) {
          JsonObject response = new JsonObject();
          try {
            JsonObject request = JsonParser.parseString(line).getAsJsonObject();
            String action = request.get("action").getAsString();
            if (action.equals("close")) {
              connection.rollback();
              System.out.println("{\"ok\":true}");
              break;
            }
            if (action.equals("commit")) connection.commit();
            else if (action.equals("rollback")) connection.rollback();
            else if (action.equals("metadata")) {
              JsonArray tables = new JsonArray();
              DatabaseMetaData md = connection.getMetaData();
              try (ResultSet rs =
                  md.getTables(null, "PUBLIC", "%", new String[] {"TABLE", "BASE TABLE"})) {
                while (rs.next()) {
                  String table = rs.getString("TABLE_NAME");
                  JsonObject t = new JsonObject();
                  t.addProperty("name", table);
                  JsonArray columns = new JsonArray();
                  try (ResultSet cs = md.getColumns(null, "PUBLIC", table, "%")) {
                    while (cs.next()) {
                      JsonObject col = new JsonObject();
                      col.addProperty("name", cs.getString("COLUMN_NAME"));
                      col.addProperty("type", cs.getString("TYPE_NAME"));
                      col.addProperty(
                          "extra",
                          "YES".equals(cs.getString("IS_GENERATEDCOLUMN")) ? "generated" : "");
                      columns.add(col);
                    }
                  }
                  TreeMap<Short, String> keys = new TreeMap<>();
                  try (ResultSet ks = md.getPrimaryKeys(null, "PUBLIC", table)) {
                    while (ks.next()) keys.put(ks.getShort("KEY_SEQ"), ks.getString("COLUMN_NAME"));
                  }
                  t.add("columns", columns);
                  t.add("pk", JSON.toJsonTree(keys.values()));
                  tables.add(t);
                }
              }
              response.add("tables", tables);
            } else if (action.equals("triggers")) {
              JsonArray tables = new JsonArray();
              try (Statement st = connection.createStatement();
                  ResultSet rs =
                      st.executeQuery(
                          "SELECT EVENT_OBJECT_TABLE FROM INFORMATION_SCHEMA.TRIGGERS")) {
                while (rs.next()) tables.add(rs.getString(1));
              }
              response.add("tables", tables);
            } else if (action.equals("unique_indexes")) {
              TreeMap<String, TreeMap<Short, String>> indexes = new TreeMap<>();
              try (ResultSet rs =
                  connection
                      .getMetaData()
                      .getIndexInfo(
                          null, "PUBLIC", request.get("table").getAsString(), true, false)) {
                while (rs.next()) {
                  String index = rs.getString("INDEX_NAME"), column = rs.getString("COLUMN_NAME");
                  if (index != null && column != null)
                    indexes
                        .computeIfAbsent(index, k -> new TreeMap<>())
                        .put(rs.getShort("ORDINAL_POSITION"), column);
                }
              }
              JsonArray result = new JsonArray();
              for (var index : indexes.values()) result.add(JSON.toJsonTree(index.values()));
              response.add("indexes", result);
            } else if (action.equals("foreign_keys")) {
              JsonArray keys = new JsonArray();
              DatabaseMetaData md = connection.getMetaData();
              try (ResultSet tables =
                  md.getTables(null, "PUBLIC", "%", new String[] {"TABLE", "BASE TABLE"})) {
                while (tables.next()) {
                  try (ResultSet rs =
                      md.getImportedKeys(null, "PUBLIC", tables.getString("TABLE_NAME"))) {
                    while (rs.next()) {
                      JsonObject key = new JsonObject();
                      key.addProperty("parent", rs.getString("PKTABLE_NAME"));
                      key.addProperty("child", rs.getString("FKTABLE_NAME"));
                      key.addProperty("parentSchema", rs.getString("PKTABLE_SCHEM"));
                      key.addProperty("childSchema", rs.getString("FKTABLE_SCHEM"));
                      key.addProperty("parentColumn", rs.getString("PKCOLUMN_NAME"));
                      key.addProperty("childColumn", rs.getString("FKCOLUMN_NAME"));
                      key.addProperty("name", rs.getString("FK_NAME"));
                      key.addProperty("sequence", rs.getShort("KEY_SEQ"));
                      key.addProperty("updateRule", rs.getShort("UPDATE_RULE"));
                      key.addProperty("deleteRule", rs.getShort("DELETE_RULE"));
                      keys.add(key);
                    }
                  }
                }
              }
              response.add("keys", keys);
            } else if (action.equals("sql")) {
              try (PreparedStatement st =
                  connection.prepareStatement(request.get("sql").getAsString())) {
                JsonArray parameters = request.getAsJsonArray("params");
                for (int i = 0; i < parameters.size(); i++)
                  st.setObject(i + 1, parameter(parameters.get(i)));
                if (st.execute()) {
                  JsonArray rows = new JsonArray();
                  try (ResultSet rs = st.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    while (rs.next()) {
                      JsonObject row = new JsonObject();
                      for (int i = 1; i <= md.getColumnCount(); i++)
                        row.add(md.getColumnLabel(i), value(rs.getObject(i)));
                      rows.add(row);
                    }
                  }
                  response.add("rows", rows);
                } else response.addProperty("count", st.getUpdateCount());
              }
            }
            response.addProperty("ok", true);
          } catch (Exception e) {
            response.addProperty("ok", false);
            response.addProperty("error", e.getClass().getSimpleName() + ": " + e.getMessage());
          }
          System.out.println(JSON.toJson(response));
        }
      }
    }
  }
}
