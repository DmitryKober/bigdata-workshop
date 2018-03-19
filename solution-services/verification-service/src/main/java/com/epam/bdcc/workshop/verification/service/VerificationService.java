package com.epam.bdcc.workshop.verification.service;

import com.epam.bdcc.workshop.common.kafka.KafkaClientFactory;
import com.epam.bdcc.workshop.common.kafka.KafkaSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@Service
public class VerificationService {

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

    private static String formPayload(String userId, String workflowId) {
        int numberOfRecordsVerified = ThreadLocalRandom.current().nextInt(1, 50_000);
        return Instant.now().toString() + " | " + userId + " | " + workflowId + " | records verified: " + numberOfRecordsVerified;
    }

    @PreDestroy
    public void shutdown() {
        kafkaSender.close();
    }
}
