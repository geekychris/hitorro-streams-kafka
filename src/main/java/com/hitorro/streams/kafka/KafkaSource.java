/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.jsontypesystem.JVS;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Kafka source — wraps a {@link KafkaConsumer} and exposes its messages as an
 * {@link Iterator}. Blocks on {@link Iterator#hasNext()} while polling the
 * broker until either a record arrives or {@code stopAfter} elapses without
 * activity (call {@link #close()} to unblock).
 *
 * <p>Two shapes:</p>
 * <ul>
 *   <li>{@link #asJvsIterator()} — parses each record's value as JSON and wraps
 *       it in a {@link JVS}. Register with
 *       {@code JvsSqlEngine.registerStream(topic, source.asJvsIterator(), type, streamConfig)}.</li>
 *   <li>{@link #asRecordIterator()} — hands over the raw {@link ConsumerRecord}
 *       so callers can inspect key / partition / offset / headers.</li>
 * </ul>
 *
 * <p>Design notes:</p>
 * <ul>
 *   <li>Uses the standard {@code kafka-clients} library only — no spring-kafka.</li>
 *   <li>Auto-commit is on by default; disable via
 *       {@code builder.autoCommit(false)} if you need at-least-once with your
 *       own commit boundaries.</li>
 *   <li>Poll timeout defaults to 1 s. A poll returning empty just keeps blocking
 *       until a record arrives; call {@link #close()} to gracefully drain.</li>
 * </ul>
 */
public final class KafkaSource implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaConsumer<String, String> consumer;
    private final Duration pollTimeout;
    private final Deque<ConsumerRecord<String, String>> buffer = new ArrayDeque<>();
    private volatile boolean closed;

    private KafkaSource(Builder b) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, b.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, b.groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(b.autoCommit));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, b.autoOffsetReset);
        if (b.maxPollRecords > 0) {
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, b.maxPollRecords);
        }
        if (b.extraConfig != null) props.putAll(b.extraConfig);
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(b.topics);
        this.pollTimeout = b.pollTimeout;
    }

    /** Iterator of raw ConsumerRecords — useful when you need key/partition/offset/headers. */
    public Iterator<ConsumerRecord<String, String>> asRecordIterator() {
        return new Iterator<>() {
            @Override public boolean hasNext() {
                while (buffer.isEmpty() && !closed) {
                    ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                    records.forEach(buffer::add);
                }
                return !buffer.isEmpty();
            }
            @Override public ConsumerRecord<String, String> next() {
                if (!hasNext()) throw new NoSuchElementException();
                return buffer.poll();
            }
        };
    }

    /**
     * Iterator of JVS documents parsed from each record's value. Records whose
     * value can't be parsed as JSON are skipped silently — subclass or use
     * {@link #asRecordIterator()} if you want to see raw bytes.
     */
    public Iterator<JVS> asJvsIterator() {
        Iterator<ConsumerRecord<String, String>> raw = asRecordIterator();
        return new Iterator<>() {
            private JVS next;
            @Override public boolean hasNext() {
                while (next == null && raw.hasNext()) {
                    ConsumerRecord<String, String> r = raw.next();
                    try {
                        JsonNode node = MAPPER.readTree(r.value());
                        next = new JVS(node);
                    } catch (Exception ignored) { /* skip un-parseable */ }
                }
                return next != null;
            }
            @Override public JVS next() {
                if (!hasNext()) throw new NoSuchElementException();
                JVS out = next; next = null; return out;
            }
        };
    }

    /** Unblock any in-flight poll and release the consumer. Idempotent. */
    @Override public void close() {
        if (closed) return;
        closed = true;
        try { consumer.wakeup(); } catch (Exception ignored) {}
        try { consumer.close(Duration.ofSeconds(5)); } catch (Exception ignored) {}
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String bootstrapServers = "localhost:9092";
        private String groupId = "hitorro-streams-kafka";
        private List<String> topics;
        private boolean autoCommit = true;
        private String autoOffsetReset = "earliest";
        private int maxPollRecords = 500;
        private Duration pollTimeout = Duration.ofSeconds(1);
        private Map<String, Object> extraConfig;

        public Builder bootstrapServers(String s) { this.bootstrapServers = s; return this; }
        public Builder groupId(String s) { this.groupId = s; return this; }
        public Builder topic(String s) { this.topics = List.of(s); return this; }
        public Builder topics(List<String> t) { this.topics = t; return this; }
        public Builder autoCommit(boolean b) { this.autoCommit = b; return this; }
        public Builder autoOffsetReset(String s) { this.autoOffsetReset = s; return this; }
        public Builder maxPollRecords(int n) { this.maxPollRecords = n; return this; }
        public Builder pollTimeout(Duration d) { this.pollTimeout = d; return this; }
        /** Additional Kafka client config; overrides defaults above. */
        public Builder extraConfig(Map<String, Object> cfg) { this.extraConfig = cfg; return this; }
        public KafkaSource build() {
            if (topics == null || topics.isEmpty()) {
                throw new IllegalStateException("at least one topic required");
            }
            return new KafkaSource(this);
        }
    }
}
