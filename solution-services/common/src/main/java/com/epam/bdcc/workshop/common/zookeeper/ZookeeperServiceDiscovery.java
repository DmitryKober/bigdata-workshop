package com.epam.bdcc.workshop.common.zookeeper;

import com.epam.bdcc.workshop.common.model.ServiceDetails;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Created by Dmitrii_Kober on 3/14/2018.
 */
@Service
public class ZookeeperServiceDiscovery implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServiceDiscovery.class);

    @Value("${zookeeper.root.namespace}") private String zookeeperRootNamespace;
    @Value("${zookeeper.host}") private String zookeeperHost;
    private CuratorFramework curatorClient;
    private ServiceDiscovery serviceDiscovery;

    @PostConstruct
    private void init() throws Exception {
        curatorClient = CuratorFrameworkFactory.newClient(zookeeperHost, new ExponentialBackoffRetry(1000, 3));
        curatorClient.start();

        serviceDiscovery = ServiceDiscoveryBuilder
                .builder(ServiceDetails.class)
                .client(curatorClient)
                .basePath(zookeeperRootNamespace)
                .build();
        serviceDiscovery.start();
    }

    public Optional<ServiceInstance<ServiceDetails>> getServiceInstance(String serviceName) throws Exception {
        return new ArrayList(serviceDiscovery.queryForInstances(serviceName)).stream().findAny();
    }

    public void registerService(String serviceHost, int servicePort, String serviceName, String serviceContext, String serviceDescription) throws Exception {
        ServiceInstance<ServiceDetails> serviceInstance = ServiceInstance.<ServiceDetails>builder()
                .name(serviceName)
                .payload(new ServiceDetails(serviceDescription))
                .port(servicePort)
                .uriSpec(new UriSpec(String.format( "{scheme}://%s:{port}%s", serviceHost, serviceContext)))
                .build();
        serviceDiscovery.registerService(serviceInstance);
    }

    @PreDestroy
    public void close() {
        try {
            serviceDiscovery.close();
            curatorClient.close();
        }
        catch (IOException e) {
            LOG.warn("Error while talking to ZooKeeper", e);
        }
    }
}
