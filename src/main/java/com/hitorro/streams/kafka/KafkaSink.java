/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.Sink;
import com.hitorro.util.io.StoreException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

/**
 * Kafka sink — implements {@link Sink}{@code <JsonNode>}. Each incoming row is
 * serialized as JSON and published to a configured Kafka topic.
 *
 * <p>The record's <b>key</b> comes from a caller-supplied {@link Function}
 * over the row (defaults to {@code null} — round-robin partitioning). The
 * <b>value</b> is the row's JSON text.</p>
 *
 * <p>Uses the standard {@code kafka-clients} producer with
 * {@link StringSerializer}. Flushes on {@link #stop()}; safe to reuse a single
 * sink for the lifetime of your pipeline.</p>
 *
 * <p>Pairs with {@link com.hitorro.jvssql.PreparedQuery#execute(Sink)}:</p>
 * <pre>{@code
 *   PreparedQuery q = engine.compile(sql);
 *   q.execute(KafkaSink.builder()
 *       .bootstrapServers("localhost:9092")
 *       .topic("enriched-events")
 *       .keyExtractor(row -> row.path("user_id").asText())
 *       .build());
 * }</pre>
 */
public final class KafkaSink implements Sink<JsonNode> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Function<JsonNode, String> keyExtractor;

    private KafkaSink(Builder b) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, b.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, b.acks);
        if (b.extraConfig != null) props.putAll(b.extraConfig);
        this.producer = new KafkaProducer<>(props);
        this.topic = b.topic;
        this.keyExtractor = b.keyExtractor;
    }

    @Override public boolean init(JsonNode config) { return true; }

    @Override public boolean start() { return true; }

    @Override public boolean add(JsonNode row) throws IOException, StoreException {
        try {
            String key = keyExtractor == null ? null : keyExtractor.apply(row);
            String value = MAPPER.writeValueAsString(row);
            producer.send(new ProducerRecord<>(topic, key, value));
            return true;
        } catch (Exception e) {
            throw new IOException("KafkaSink.add failed: " + e.getMessage(), e);
        }
    }

    @Override public boolean stop() {
        try { producer.flush(); } catch (Exception ignored) {}
        return true;
    }

    @Override public void close() throws IOException {
        try { producer.close(Duration.ofSeconds(5)); } catch (Exception ignored) {}
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String bootstrapServers = "localhost:9092";
        private String topic;
        private String acks = "all";
        private Function<JsonNode, String> keyExtractor;
        private Map<String, Object> extraConfig;

        public Builder bootstrapServers(String s) { this.bootstrapServers = s; return this; }
        public Builder topic(String t) { this.topic = t; return this; }
        public Builder acks(String a) { this.acks = a; return this; }
        public Builder keyExtractor(Function<JsonNode, String> f) { this.keyExtractor = f; return this; }
        public Builder extraConfig(Map<String, Object> cfg) { this.extraConfig = cfg; return this; }
        public KafkaSink build() {
            if (topic == null) throw new IllegalStateException("topic required");
            return new KafkaSink(this);
        }
    }
}
