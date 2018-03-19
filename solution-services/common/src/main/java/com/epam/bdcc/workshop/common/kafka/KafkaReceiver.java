package com.epam.bdcc.workshop.common.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Created by Dmitrii_Kober on 3/15/2018.
 */

public class KafkaReceiver implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaReceiver.class);

    private Consumer<String, String> consumer;
    private java.util.function.Consumer<ConsumerRecord<String, String>> callback;
    private String topic;
    private String consumerGroup;

    public KafkaReceiver(KafkaClientFactory kafkaClientFactory, String topic, String consumerGroup) {
        this(
                kafkaClientFactory,
                topic,
                consumerGroup,
                record -> LOG.info("Record with key:'{}' and value:'{}' is received from topic:'{}'", record.key(), record.value(), record.topic())
        );
    }

    public KafkaReceiver(KafkaClientFactory kafkaClientFactory, String topic, String consumerGroup, java.util.function.Consumer<ConsumerRecord<String, String>> callback) {
        this.consumer = kafkaClientFactory.consumer(consumerGroup);
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.callback = callback;
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                records.forEach(callback);
            }
        }
        catch (WakeupException e) {
            // ignore for shutdown
        }
        finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
