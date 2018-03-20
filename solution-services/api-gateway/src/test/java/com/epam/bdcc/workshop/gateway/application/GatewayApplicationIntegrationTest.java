package com.epam.bdcc.workshop.gateway.application;

import com.epam.bdcc.workshop.common.kafka.KafkaClientFactory;
import com.epam.bdcc.workshop.common.kafka.KafkaReceiver;
import com.epam.bdcc.workshop.common.zookeeper.ZookeeperServiceDiscovery;
import com.epam.bdcc.workshop.gateway.controller.GatewayController;
import com.epam.bdcc.workshop.gateway.kafka.EmbeddedKafkaCluster;
import com.epam.bdcc.workshop.gateway.zookeeper.EmbeddedZookeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Created by Dmitrii_Kober on 3/14/2018.
 */
@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class GatewayApplicationIntegrationTest {

    private static final int ZOOKEEPER_PORT = 2283;
    private static final int KAFKA_PORT = 9094;
    private static final String ZOOKEEPER_ROOT_NAMESPACE = "workshop-services";
    private static final String KAFKA_USER_ACTIVITY_CONSUMER_TOPIC = "gateway.user.activity.topic";
    private static final String KAFKA_USER_ACTIVITY_CONSUMER_GROUP = "gateway-user-activity-consumer-group";
    private static final String GATEWAY_ReQUEST = "{\n" +
            "  \"userIds\": [\"Dmitrii\",\"Jennifer\", \"Daniel\"],\n" +
            "  \"numberOfRequests\": 1,\n" +
            "  \"workflows\": [\n" +
            "    {\n" +
            "      \"steps\": [\n" +
            "        {\"serviceName\": \"database\", \"invocationDelay\":0},\n" +
            "        {\"serviceName\": \"verifier\", \"invocationDelay\":0},\n" +
            "        {\"serviceName\": \"generator\", \"invocationDelay\":0}\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"steps\": [\n" +
            "        {\"serviceName\": \"database\", \"invocationDelay\":1000},\n" +
            "        {\"serviceName\": \"verifier\", \"invocationDelay\":1000},\n" +
            "        {\"serviceName\": \"generator\", \"invocationDelay\":1000}\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Value("${local.server.port}") private String localServerPort;
    @SpyBean private RestTemplate restTemplate;
    @Autowired private KafkaClientFactory kafkaClientFactory;
    @Autowired private ZookeeperServiceDiscovery zookeeperServiceDiscovery;

    @BeforeClass
    public static void init() throws Exception {
        setUpZookeeper();
        setUpKafka();
    }

    private static void setUpZookeeper() throws Exception {
        EmbeddedZookeeper.instance(ZOOKEEPER_PORT).start();
        EmbeddedZookeeper.registerService(ZOOKEEPER_ROOT_NAMESPACE, ZOOKEEPER_ROOT_NAMESPACE, "database", 1011, "database #1");
        EmbeddedZookeeper.registerService(ZOOKEEPER_ROOT_NAMESPACE, ZOOKEEPER_ROOT_NAMESPACE, "database", 1012, "database #2");
        EmbeddedZookeeper.registerService(ZOOKEEPER_ROOT_NAMESPACE, ZOOKEEPER_ROOT_NAMESPACE, "verifier", 1111, "verifier #1");
        EmbeddedZookeeper.registerService(ZOOKEEPER_ROOT_NAMESPACE, ZOOKEEPER_ROOT_NAMESPACE, "verifier", 1112, "verifier #2");
        EmbeddedZookeeper.registerService(ZOOKEEPER_ROOT_NAMESPACE, ZOOKEEPER_ROOT_NAMESPACE, "verifier", 1113, "verifier #3");
        EmbeddedZookeeper.registerService(ZOOKEEPER_ROOT_NAMESPACE, ZOOKEEPER_ROOT_NAMESPACE, "generator", 1211, "generator #1");
    }

    private static void setUpKafka() {
        EmbeddedKafkaCluster.instance("localhost:" + ZOOKEEPER_PORT, KAFKA_PORT);
        EmbeddedKafkaCluster.instance().startup();
    }

    @Before
    public void setUp() {
        // no to cause a self-invocation on
        doAnswer(invocationOnMock -> null)
                .when(restTemplate).postForEntity(AdditionalMatchers.not(ArgumentMatchers.contains(GatewayController.GATEWAY_SERVICE)), any(), any());
    }

    @Test
    public void whenRequestReceived_thenRequiredServicesAreInvoked() throws InterruptedException, IOException {
        List<String> kafkaMessagesReceived = new LinkedList<>();

        KafkaReceiver kafkaReceiver = givenKafkaListenerStarted(kafkaMessagesReceived);
        whenRESTRequestSentToGatewayService();
        thenGatewayServiceLogsEachWorkflowStepInvocation();
        thenGatewayServiceSendsUserIdsToKafka(kafkaReceiver, kafkaMessagesReceived);
    }

    private void thenGatewayServiceSendsUserIdsToKafka(KafkaReceiver kafkaReceiver, List<String> kafkaMessagesReceived) {
        kafkaReceiver.shutdown();
        assertFalse("No messages received.",  kafkaMessagesReceived.isEmpty());
    }

    private void thenGatewayServiceLogsEachWorkflowStepInvocation() throws IOException {
//        assertTrue("Audit log is empty.", Files.size(Paths.get("audit.log")) > 0);
    }

    private void whenRESTRequestSentToGatewayService() throws InterruptedException {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<?> request = new HttpEntity<>(GATEWAY_ReQUEST, headers);
        restTemplate.postForEntity("http://localhost:" + localServerPort + GatewayController.GATEWAY_SERVICE, request, Void.class);
        Thread.sleep(5000);
    }

    private KafkaReceiver givenKafkaListenerStarted(List<String> kafkaMessagesReceived) {
        KafkaReceiver kafkaReceiver = new KafkaReceiver(
                kafkaClientFactory,
                KAFKA_USER_ACTIVITY_CONSUMER_TOPIC,
                KAFKA_USER_ACTIVITY_CONSUMER_GROUP,
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
