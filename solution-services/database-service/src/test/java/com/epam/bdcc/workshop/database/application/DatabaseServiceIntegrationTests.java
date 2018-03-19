package com.epam.bdcc.workshop.database.application;

import com.epam.bdcc.workshop.common.kafka.KafkaClientFactory;
import com.epam.bdcc.workshop.common.kafka.KafkaReceiver;
import com.epam.bdcc.workshop.common.zookeeper.ZookeeperServiceDiscovery;
import com.epam.bdcc.workshop.database.controller.DatabaseController;
import com.epam.bdcc.workshop.gateway.kafka.EmbeddedKafkaCluster;
import com.epam.bdcc.workshop.gateway.zookeeper.EmbeddedZookeeper;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by Dmitrii_Kober on 3/14/2018.
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DatabaseServiceIntegrationTests {

    private static final int ZOOKEEPER_PORT = 2283;

    private static final int KAFKA_PORT = 9094;
    private static final String KAFKA_RESOURCE_UTILIZATION_CONSUMER_TOPIC = "resource.utilization.topic";
    private static final String KAFKA_RESOURCE_UTILIZATION_CONSUMER_GROUP = "database-test-consumer-group";

    @Value("${local.server.port}") private int localServerPort;
    @Autowired private TestRestTemplate restTemplate;
    @Autowired private KafkaClientFactory kafkaClientFactory;
    @Autowired private ZookeeperServiceDiscovery zookeeperServiceDiscovery;

    @BeforeClass
    public static void setUp() throws Exception {
        setUpZookeeper();
        setUpKafka();
    }

    private static void setUpZookeeper() throws Exception {
        EmbeddedZookeeper.instance(ZOOKEEPER_PORT).start();
    }

    private static void setUpKafka() {
        EmbeddedKafkaCluster.instance("localhost:" + ZOOKEEPER_PORT, KAFKA_PORT);
        EmbeddedKafkaCluster.instance().startup();
    }

    @Test
    public void whenInvokingController_thenResourceConsumptionProvided() throws Exception {
        List<String> kafkaMessagesReceived = new LinkedList<>();
        KafkaReceiver kafkaReceiver = givenKafkaListenerStarted(kafkaMessagesReceived);

        Map<String, String> requestPayload = new HashMap<>();
        requestPayload.put("userId", "Dmitrii");
        requestPayload.put("workflowId", "my-process-id");

        HttpEntity<?> request = new HttpEntity<>(requestPayload);
        ResponseEntity<Void> responseEntity = restTemplate.postForEntity(
                "http://localhost:" + localServerPort + DatabaseController.CONTEXT + DatabaseController.SERVICE,
                request,
                Void.class
        );
        Thread.sleep(5000);
        assertEquals("Response is not successful!", HttpStatus.OK, responseEntity.getStatusCode());
        assertFalse("No resource utilization information is received!", kafkaMessagesReceived.isEmpty());

        kafkaReceiver.shutdown();
    }

    private KafkaReceiver givenKafkaListenerStarted(List<String> kafkaMessagesReceived) {
        KafkaReceiver kafkaReceiver = new KafkaReceiver(
                kafkaClientFactory,
                KAFKA_RESOURCE_UTILIZATION_CONSUMER_TOPIC,
                KAFKA_RESOURCE_UTILIZATION_CONSUMER_GROUP,
                record -> kafkaMessagesReceived.add(record.value())
        );
        Thread thread = new Thread(kafkaReceiver);
        thread.start();
        return kafkaReceiver;
    }

    @After
    public void tearDown() throws Exception {
        zookeeperServiceDiscovery.close();
        EmbeddedKafkaCluster.instance().shutdown();
        Thread.sleep(1000);
        EmbeddedZookeeper.instance().stop();
    }
}
