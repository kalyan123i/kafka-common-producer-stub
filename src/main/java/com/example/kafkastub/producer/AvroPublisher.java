package com.example.kafkastub.producer;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class AvroPublisher {

    private static final Logger log = LoggerFactory.getLogger(AvroPublisher.class);

    private final StreamBridge streamBridge;

    public AvroPublisher(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    /**
     * Sends one Avro record to the given topic. The {@code topic} argument is passed as the
     * StreamBridge binding name; Spring Cloud Stream creates a dynamic producer binding using
     * the default producer config (Avro serializer, schema registry URL) declared in application.yml.
     */
    public void publish(String topic, GenericRecord record, String messageKey) {
        MessageBuilder<GenericRecord> builder = MessageBuilder.withPayload(record);
        if (messageKey != null) {
            builder.setHeader(KafkaHeaders.KEY, messageKey);
        }
        Message<GenericRecord> message = builder.build();
        boolean sent = streamBridge.send(topic, message);
        if (!sent) {
            throw new IllegalStateException("StreamBridge failed to send message to topic '" + topic + "'");
        }
        log.info("Published 1 record to topic='{}' key='{}' schema={}", topic, messageKey, record.getSchema().getFullName());
    }
}
