/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.kafka;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.util.json.JsonMappers;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the Kafka wire format to what 3.0.2 sent and accepted on Jackson 2: the record value both
 * sinks write for parsed and code-built rows (number subtypes, escapes, binary, POJOs, rows that
 * cannot be written), what {@link KafkaSource} makes of 30 record values (valid, trailing content,
 * malformed, deep nesting, empty), and the record key the jvssql sink extracts.
 *
 * <p>{@code jackson2-kafka-wire.txt} was produced by running the same cases against the 3.0.2
 * classes on Jackson 2.18, through those classes' own mappers and key extractor.</p>
 */
class Jackson2WireCompatTest {
  static final String[] ENCODERS = {"KafkaSink", "util.KafkaSink"};
  static final String[] DECODERS = {"KafkaSource"};
  static final String[] KEYERS = {"util.KafkaSink"};
  static final String UTIL = "com.hitorro.util.core.iterator.sinks.kafka.KafkaSink";

  static Object util(String name, Object... args) throws Throwable {
    Class<?> c = Class.forName(UTIL);
    Method m = name.equals("encode")
        ? c.getDeclaredMethod(name, tools.jackson.databind.JsonNode.class)
        : c.getDeclaredMethod(name, tools.jackson.databind.JsonNode.class, String.class);
    m.setAccessible(true);
    try { return m.invoke(null, args); } catch (InvocationTargetException e) { throw e.getCause(); }
  }
  static tools.jackson.databind.JsonNode parse(String s) { return JsonMappers.json().readTree(s); }
  static String encode(String enc, tools.jackson.databind.JsonNode n) throws Throwable {
    return enc.equals("KafkaSink") ? KafkaSink.encode(n) : (String) util("encode", n);
  }
  static String decode(String dec, String value) {
    JVS j = KafkaSource.decode(value);
    return j == null ? "skip" : sig(j.getJsonNode());
  }
  static String key(String ke, tools.jackson.databind.JsonNode row, String path) throws Throwable {
    return (String) util("extract", row, path);
  }
  /**
   * Deliberate differences from Jackson 2 (everything else must match the golden file exactly):
   * <ul>
   * <li>java.time and Optional are built into Jackson 3. A row holding them in a POJO node could not
   * be sent by 3.x (IOException); it is now sent, the Instant as decimal epoch seconds (what Jackson 2
   * wrote with JavaTimeModule). Same decision as hitorro-util {@code Http}.</li>
   * <li>Jackson 3's default maximum nesting depth is 500 (Jackson 2.18: 1000), and hitorro-core's
   * {@code JsonMappers} keeps it, so a value nested 600 deep is now skipped. If core raises the limit
   * to 1000 this entry fails and should be removed.</li>
   * </ul>
   */
  static final Map<String, String> ACCEPTED = Map.of(
      "KafkaSink.built.pojo.instant", "{\"p\":1700000000.123000000}",
      "util.KafkaSink.built.pojo.instant", "{\"p\":1700000000.123000000}",
      "KafkaSink.built.pojo.optional", "{\"p\":\"v\"}",
      "util.KafkaSink.built.pojo.optional", "{\"p\":\"v\"}",
      "KafkaSource.in28", "skip");
  // ---- shared by the Jackson 2 generator and the Jackson 3 test (tools.jackson = jackson package) ----
  public record Rec(int b, String a, java.util.Date d, java.util.List<String> l) {}
  public static final class Bean { private int z = 1; private String y = "y"; public int getZ() { return z; } public String getY() { return y; } }

  static String nest(int depth) {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < depth; i++) b.append("[");
    for (int i = 0; i < depth; i++) b.append("]");
    return b.toString();
  }

  static final String[] TEXT_ROWS = {
    "{\"b\":1,\"a\":\"x\",\"n\":null,\"arr\":[1,2.5,\"s\",true,null],\"o\":{\"z\":1,\"y\":[]},\"big\":12345678901234567890,\"dec\":1.10,\"neg\":-0.0,\"l\":-9223372036854775808}",
    "{\"s\":\"\\u0000\\u001f\\\"\\\\/ \\u00e9 \\ud83d\\ude00 \\u2028 <&>'\"}",
    "{\"e\":1e400,\"f\":1.0E-5,\"g\":123456789.123456789,\"h\":0.1}",
    "[1,{\"a\":[]},\"x\"]",
    "\"just text\"",
    "42",
    "null",
    "{}"
  };

  static final String[] DECODE_INPUTS = {
    "{\"a\":1,\"b\":[true,null,\"x\"],\"c\":{\"d\":1.10}}",
    "{\"a\":1}{\"b\":2}", "{\"a\":1} trailing", "{\"a\":1}\n", "  {\"a\":1}  ",
    "", "   ", "nul", "null", "[1,2]", "\"s\"", "12345678901234567890", "1.10", "-0", "1e400",
    "{\"a\":1,}", "{'a':1}", "{a:1}", "{\"a\":NaN}", "{\"a\":01}", "/* c */ {\"a\":1}", "{\"a\":\"\u0001\"}",
    "\uFEFF{\"a\":1}", "{\"a\":1,\"a\":2}", "{\"a\":", "[1,2", "{\"a\":\"\\ud83d\\ude00\"}",
    nest(400), nest(600), nest(1200)
  };

  static final String[] KEY_PATHS = {"id", "n", "d", "b", "nul", "o", "a", "o.k", "o.k.x", "missing", "o.missing.x", "big", "neg"};
  static final String KEY_ROW = "{\"id\":\"doc-1\",\"n\":42,\"d\":1.10,\"b\":true,\"nul\":null,\"o\":{\"k\":\"v\",\"n\":[1]},\"a\":[1,2],\"big\":12345678901234567890,\"neg\":-0.0}";

  /** Rows that are built in code rather than parsed (number node subtypes, binary, POJOs). */
  static java.util.LinkedHashMap<String, tools.jackson.databind.JsonNode> builtRows() {
    tools.jackson.databind.node.JsonNodeFactory f = tools.jackson.databind.node.JsonNodeFactory.instance;
    java.util.LinkedHashMap<String, tools.jackson.databind.JsonNode> m = new java.util.LinkedHashMap<>();
    tools.jackson.databind.node.ObjectNode o = f.objectNode();
    o.put("bd110", new java.math.BigDecimal("1.10"));
    o.put("bd1e3", new java.math.BigDecimal("1E+3"));
    o.put("bdbig", new java.math.BigDecimal("12345678901234567890.000"));
    o.put("bi", new java.math.BigInteger("123456789012345678901234567890"));
    o.put("fl", 0.1f);
    o.put("dbl", 0.1d);
    o.put("nan", Double.NaN);
    o.put("inf", Double.NEGATIVE_INFINITY);
    o.put("lmax", Long.MAX_VALUE);
    o.put("sh", (short) 7);
    o.put("bin", new byte[]{0, 1, 2, (byte) 255, 64});
    o.put("bool", false);
    o.putNull("nul");
    o.set("arr", f.arrayNode().add(1).add("two").add(f.nullNode()).add(f.numberNode(new java.math.BigDecimal("0.00"))));
    m.put("numbers", o);
    java.util.LinkedHashMap<String, Object> pm = new java.util.LinkedHashMap<>();
    pm.put("zeta", 1); pm.put("alpha", java.util.List.of("x", 2)); pm.put("date", new java.util.Date(1700000000123L));
    m.put("pojo.map", f.objectNode().set("p", f.pojoNode(pm)));
    m.put("pojo.record", f.objectNode().set("p", f.pojoNode(new Rec(2, "a", new java.util.Date(5L), java.util.List.of("q")))));
    m.put("pojo.bean", f.objectNode().set("p", f.pojoNode(new Bean())));
    m.put("pojo.empty", f.objectNode().set("p", f.pojoNode(new Object())));
    m.put("pojo.instant", f.objectNode().set("p", f.pojoNode(java.time.Instant.ofEpochMilli(1700000000123L))));
    m.put("pojo.optional", f.objectNode().set("p", f.pojoNode(java.util.Optional.of("v"))));
    m.put("missing", tools.jackson.databind.node.MissingNode.getInstance());
    m.put("nullnode", f.nullNode());
    return m;
  }

  static String sig(tools.jackson.databind.JsonNode n) {
    if (n == null) return "<java-null>";
    if (n.isObject()) {
      StringBuilder b = new StringBuilder("{");
      for (java.util.Map.Entry<String, tools.jackson.databind.JsonNode> e : n.properties()) {
        if (b.length() > 1) b.append(',');
        b.append(e.getKey()).append(':').append(sig(e.getValue()));
      }
      return b.append('}').toString();
    }
    if (n.isArray()) {
      StringBuilder b = new StringBuilder("[");
      for (tools.jackson.databind.JsonNode c : n) { if (b.length() > 1) b.append(','); b.append(sig(c)); }
      return b.append(']').toString();
    }
    if (n.isNumber()) return n.numberType() + "(" + n.toString() + ")";
    return n.getNodeType() + "(" + n.toString() + ")";
  }

  static String err(Throwable t) {
    return t instanceof java.io.IOException ? "!IOException" : "!" + t.getClass().getSimpleName();
  }

  static String esc(String s) {
    if (s == null) return "<null>";
    if (s.length() > 300) return "len=" + s.length() + ":" + Integer.toHexString(s.hashCode());
    return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
  }

  static String generate() throws Exception {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < TEXT_ROWS.length; i++) {
      tools.jackson.databind.JsonNode n = parse(TEXT_ROWS[i]);
      for (String enc : ENCODERS) {
        String v;
        try { v = esc(encode(enc, n)); } catch (Throwable t) { v = err(t); }
        out.append(enc).append(".text").append(i).append('=').append(v).append('\n');
      }
    }
    for (java.util.Map.Entry<String, tools.jackson.databind.JsonNode> e : builtRows().entrySet()) {
      for (String enc : ENCODERS) {
        String v;
        try { v = esc(encode(enc, e.getValue())); } catch (Throwable t) { v = err(t); }
        out.append(enc).append(".built.").append(e.getKey()).append('=').append(v).append('\n');
      }
    }
    for (int i = 0; i < DECODE_INPUTS.length; i++) {
      for (String dec : DECODERS) {
        String v;
        try { v = esc(decode(dec, DECODE_INPUTS[i])); } catch (Throwable t) { v = err(t); }
        out.append(dec).append(".in").append(i).append('=').append(v).append('\n');
      }
    }
    tools.jackson.databind.JsonNode kr = parse(KEY_ROW);
    for (String k : KEY_PATHS) {
      for (String ke : KEYERS) {
        String v;
        try { v = esc(key(ke, kr, k)); } catch (Throwable t) { v = err(t); }
        out.append(ke).append(".key.").append(k).append('=').append(v).append('\n');
      }
    }
    return out.toString();
  }

  @Test
  void wireFormatMatchesJackson2() throws Throwable {
    Map<String, String> golden = new LinkedHashMap<>();
    try (InputStream in = getClass().getResourceAsStream("jackson2-kafka-wire.txt")) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        int eq = line.indexOf('=');
        if (eq > 0) golden.put(line.substring(0, eq), line.substring(eq + 1));
      }
    }
    Map<String, String> actual = new LinkedHashMap<>();
    for (String line : generate().split("\n")) {
      int eq = line.indexOf('=');
      if (eq > 0) actual.put(line.substring(0, eq), line.substring(eq + 1));
    }
    List<String> diffs = new ArrayList<>();
    for (String k : golden.keySet()) {
      String expected = ACCEPTED.containsKey(k) ? ACCEPTED.get(k) : golden.get(k);
      if (!Objects.equals(expected, actual.get(k))) diffs.add(k + "\n  expected: " + expected + (ACCEPTED.containsKey(k) ? " (accepted difference; jackson2: " + golden.get(k) + ")" : "") + "\n  now:      " + actual.get(k));
    }
    for (String k : ACCEPTED.keySet()) assertTrue(golden.containsKey(k), "accepted difference for unknown key " + k);
    for (String k : actual.keySet()) if (!golden.containsKey(k)) diffs.add(k + " (not in golden): " + actual.get(k));
    assertTrue(diffs.isEmpty(), diffs.size() + " differences:\n" + String.join("\n", diffs));
    assertEquals(77, golden.size());
  }
}
