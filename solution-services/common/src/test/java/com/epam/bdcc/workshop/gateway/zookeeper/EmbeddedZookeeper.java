package com.epam.bdcc.workshop.gateway.zookeeper;

import com.epam.bdcc.workshop.TestUtils;
import com.epam.bdcc.workshop.common.model.ServiceDetails;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;

import java.io.IOException;

/**
 * Created by Dmitrii_Kober on 3/14/2018.
 */
public class EmbeddedZookeeper {

    private static TestingServer zkTestServer;
    private static EmbeddedZookeeper instance;

    private EmbeddedZookeeper(int port) throws Exception {
        zkTestServer = new TestingServer(port);
    }

    public static EmbeddedZookeeper instance(int port) throws Exception {
        if (instance != null) {
            if (zkTestServer.getPort() == port) {
                return instance;
            }
            else {
                throw new RuntimeException("There is already a Zookeeper instance created on port " + zkTestServer.getPort());
            }
        }
        instance = new EmbeddedZookeeper(port);
        return instance;
    }

    public static EmbeddedZookeeper instance() throws Exception {
        if (instance != null) {
            return instance;
        }
        int port = TestUtils.random().nextInt(16383) + 49152;
        return instance(port);
    }

    public void start() throws Exception {
        zkTestServer.start();
    }

    public static void registerService(String namespace, String serviceContext, String serviceName, int servicePort, String serviceDescription) throws Exception {
        CuratorFramework curatorClient = CuratorFrameworkFactory.newClient(
                "localhost:" + zkTestServer.getPort(),
                new ExponentialBackoffRetry(1000, 3)
        );
        curatorClient.start();
        ServiceDiscovery<ServiceDetails> serviceDiscovery = ServiceDiscoveryBuilder
                .builder(ServiceDetails.class)
                .client(curatorClient)
                .basePath(namespace)
                .build();

        ServiceInstance<ServiceDetails> serviceInstance = ServiceInstance.<ServiceDetails>builder()
                .name(serviceName)
                .payload(new ServiceDetails(serviceDescription))
                .port(servicePort)
                .uriSpec(new UriSpec(String.format("{scheme}://localhost:{port}/%s/%s", serviceContext, serviceName)))
                .build();
        serviceDiscovery.registerService(serviceInstance);
    }

    public void stop() throws IOException {
        zkTestServer.stop();
    }

}
