/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.util.core.iterator.sinks.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.JsonNodeSinkBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Kafka producer sink. Sends each row as a String key / String JSON value
 * record to {@code topic}. {@code keyExpr} is an optional dotted path
 * into the row — if unset, records land round-robin across partitions.
 *
 * <p>Lives in {@code hitorro-streams-kafka} — usable by any caller that
 * has Kafka on the classpath and wants a {@link com.hitorro.util.core.iterator.sinks.Sink}
 * over it. No mesh dependency.</p>
 */
public final class KafkaSink extends JsonNodeSinkBase {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String bootstrap;
    private final String topic;
    private final String keyExpr;
    private KafkaProducer<String, String> producer;

    public KafkaSink(String bootstrap, String topic, String keyExpr) {
        this.bootstrap = bootstrap;
        this.topic     = topic;
        this.keyExpr   = keyExpr;
    }

    public String topic() { return topic; }

    @Override
    public boolean start() throws IOException {
        Properties cfg = new Properties();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,     bootstrap);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.LINGER_MS_CONFIG,              5);   // small batching
        producer = new KafkaProducer<>(cfg);
        return true;
    }

    @Override
    protected void writeRow(JsonNode row) throws IOException {
        if (producer == null) start();
        String key = keyExpr == null ? null : extract(row, keyExpr);
        String val = JSON.writeValueAsString(row);
        producer.send(new ProducerRecord<>(topic, key, val));
    }

    @Override
    public void close() throws IOException {
        try { if (producer != null) producer.flush(); } catch (Exception ignored) { }
        try { if (producer != null) producer.close(); } catch (Exception ignored) { }
        producer = null;
    }

    private static String extract(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return null;
            cur = cur.get(seg);
        }
        return (cur == null || cur.isNull()) ? null : cur.asText();
    }
}
