package com.epam.bdcc.workshop.gateway.service;

import com.epam.bdcc.workshop.common.kafka.KafkaClientFactory;
import com.epam.bdcc.workshop.common.kafka.KafkaSender;
import com.epam.bdcc.workshop.common.model.ServiceDetails;
import com.epam.bdcc.workshop.common.zookeeper.ZookeeperServiceDiscovery;
import com.epam.bdcc.workshop.gateway.model.WorkflowContainer;
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@Service
public class WorkflowService {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowService.class);

    @Value("${zookeeper.root.namespace}") String zookeeperNamespace;
    @Value("${kafka.gateway.user.activity.topic}") String gatewayUserActivityTopic;
    @Autowired private ZookeeperServiceDiscovery serviceDiscovery;
    @Autowired private RestTemplate restTemplate;
    @Autowired private KafkaClientFactory kafkaClientFactory;
    private KafkaSender kafkaSender;

    private final ExecutorService executorService;

    public WorkflowService() {
        executorService = Executors.newFixedThreadPool(10);
    }

    @PostConstruct
    public void init() {
        kafkaSender = new KafkaSender(kafkaClientFactory);
    }

    public void handle(WorkflowContainer workflowContainer) {
        System.out.println("Start handling request" + workflowContainer.getWorkflows());
        LOG.info("Start handling request " + workflowContainer.getWorkflows());
        for (WorkflowContainer.Workflow workflow : workflowContainer.getWorkflows()) {
            int actualNumberOfRequests = ThreadLocalRandom.current().nextInt(workflowContainer.numberOfRequests) + 1;

            System.out.println("actualNumberOfRequests " + actualNumberOfRequests);
            LOG.info("actualNumberOfRequests " + actualNumberOfRequests);

            for (int i = 0; i < actualNumberOfRequests; i++) {
                List<String> userIds = workflowContainer.getUserIds();
                String userId = userIds.get(ThreadLocalRandom.current().nextInt(userIds.size()));
                executorService.execute(() -> doHandleWorkflow(userId, workflow));
            }
        }
    }

    private void doHandleWorkflow(String userId, WorkflowContainer.Workflow workflow) {
        String workflowId = UUID.randomUUID().toString();

        for (WorkflowContainer.Workflow.Step step : workflow.getSteps()) {
            try {
                Thread.sleep(step.invocationDelay);
                Optional<ServiceInstance<ServiceDetails>> stepService = serviceDiscovery.getServiceInstance(step.serviceName);
                stepService.ifPresent(serviceInstance -> {
                    Map<String, String> requestPayload = new HashMap<>();
                    requestPayload.put("userId", userId);
                    requestPayload.put("workflowId", workflowId);

                    HttpEntity<Map> request = new HttpEntity<>(requestPayload);
                    restTemplate.postForEntity(serviceInstance.buildUriSpec(), request, Void.class);

                    LOG.info("[{}] [{}] [{}]", userId, workflowId, step.serviceName);

                    kafkaSender.send(gatewayUserActivityTopic, userId);
                });
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            catch (Exception e) {
                LOG.error("'{}' workflow invocation failed", workflowId, e);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        kafkaSender.close();
    }
}
