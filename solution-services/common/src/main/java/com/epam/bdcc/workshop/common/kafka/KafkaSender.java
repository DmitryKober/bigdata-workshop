package com.epam.bdcc.workshop.common.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Created by Dmitrii_Kober on 3/15/2018.
 */
public class KafkaSender {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSender.class);

    private BiConsumer<RecordMetadata, Exception> callback;
    private Producer<String, String> producer;

    public KafkaSender(KafkaClientFactory kafkaClientFactory) {
        this(
                kafkaClientFactory,
                (recordMetadata, e) -> {
                    if (e != null) {
                        LOG.error("An error occurred while sending a message to '{}' topic", recordMetadata.topic(), e);
                    }
                    else {
                        LOG.info("Sent payload to '{}' partition of topic='{}'", recordMetadata.toString(), recordMetadata.topic());
                    }
                }
        );
    }

    public KafkaSender(KafkaClientFactory kafkaClientFactory, BiConsumer<RecordMetadata, Exception> callback) {
        this.producer = kafkaClientFactory.producer();
        this.callback = callback;
    }

    public void send(String topic, String payload) {
        producer.send(
                new ProducerRecord<>(topic, UUID.randomUUID().toString(), payload),
                (recordMetadata, e) -> LOG.info("Sent payload='{" + payload + "}' to topic='{" + topic + "}'")
        );
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
