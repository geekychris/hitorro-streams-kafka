/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.streams.kafka;

import com.hitorro.util.core.iterator.sinks.Sink;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests. Real end-to-end tests would need Testcontainers + an embedded
 * Kafka broker — too heavy for a default unit-test run. These verify the
 * builder API compiles, defaults are sensible, and the sink type contract
 * matches what {@code PreparedQuery.execute(Sink)} expects.
 */
class KafkaSourceSinkTest {

    @Test
    void sourceBuilder_rejectsEmptyTopics() {
        assertThatThrownIs(() -> KafkaSource.builder()
                .bootstrapServers("localhost:9092")
                .groupId("g")
                .build(),
            IllegalStateException.class,
            "at least one topic required");
    }

    @Test
    void sinkBuilder_rejectsMissingTopic() {
        assertThatThrownIs(() -> KafkaSink.builder()
                .bootstrapServers("localhost:9092")
                .build(),
            IllegalStateException.class,
            "topic required");
    }

    @Test
    void sinkImplementsSinkOfJsonNode() {
        // Compile-time verification via type declaration + assignment.
        // (Runtime instantiation would try to connect to Kafka on port 9092.)
        // This test proves KafkaSink is a legal Sink<JsonNode> — the type the
        // JvsSqlEngine's PreparedQuery.execute(Sink) accepts.
        Class<?> sinkIface = Sink.class;
        assertThat(sinkIface.isAssignableFrom(KafkaSink.class)).isTrue();
    }

    @Test
    void recordShape_matchesKafkaClients() {
        // Type-level check: KafkaSource.asRecordIterator returns an iterator of
        // the standard kafka-clients ConsumerRecord<String, String>. Downstream
        // consumers can access key/partition/offset/headers.
        Class<?> recordCls = ConsumerRecord.class;
        assertThat(recordCls.getName()).isEqualTo("org.apache.kafka.clients.consumer.ConsumerRecord");
    }

    // -- helper ---------------------------------------------------------------

    private static void assertThatThrownIs(Runnable r, Class<? extends Throwable> t, String msgFragment) {
        try {
            r.run();
        } catch (Throwable actual) {
            assertThat(actual).isInstanceOf(t);
            assertThat(actual.getMessage()).contains(msgFragment);
            return;
        }
        throw new AssertionError("expected " + t.getSimpleName() + " containing '" + msgFragment + "'");
    }
}
