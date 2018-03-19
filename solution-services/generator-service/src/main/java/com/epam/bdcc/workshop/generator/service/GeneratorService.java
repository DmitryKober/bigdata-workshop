package com.epam.bdcc.workshop.generator.service;

import com.epam.bdcc.workshop.common.kafka.KafkaClientFactory;
import com.epam.bdcc.workshop.common.kafka.KafkaSender;
import com.epam.bdcc.workshop.common.model.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@Service
public class GeneratorService {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    @Value("${service.name}") private String serviceName;
    @Value("${kafka.resource.utilization.info.topic}") private String resourceUtilizationInfoTopic;
    @Autowired KafkaClientFactory kafkaClientFactory;
    private KafkaSender kafkaSender;

    @PostConstruct
    public void init() {
        kafkaSender = new KafkaSender(kafkaClientFactory);
    }

    public void handle(String userId, String workflowId) {
        String payload = formPayload(userId, workflowId);
        kafkaSender.send(resourceUtilizationInfoTopic, payload);
    }

    private  String formPayload(String userId, String workflowId) {
        Pair<String, String> timeBounds = new Pair<>(
                DATE_TIME_FORMATTER.format(Instant.now()),
                DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(Instant.now().toEpochMilli() + ThreadLocalRandom.current().nextLong(1000, 10000))));
        double avgCpuTime = ThreadLocalRandom.current().nextDouble(0, 1);
        return "[" + serviceName + "] " + timeBounds + " | " + userId + " | " + workflowId + " | avg cpu time: " + avgCpuTime;
    }

    @PreDestroy
    public void shutdown() {
        kafkaSender.close();
    }
}
