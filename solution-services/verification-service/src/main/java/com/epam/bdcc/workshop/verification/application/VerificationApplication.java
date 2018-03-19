package com.epam.bdcc.workshop.verification.application;

import com.epam.bdcc.workshop.common.zookeeper.ZookeeperServiceDiscovery;
import com.epam.bdcc.workshop.verification.controller.VerificationController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.ComponentScan;

import java.net.InetAddress;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@SpringBootApplication
@ComponentScan("com.epam.bdcc.workshop")
public class VerificationApplication implements ApplicationListener<ServletWebServerInitializedEvent> {

    @Value("${service.name}") private String serviceName;
    @Value("${service.description}") private String serviceDescription;
    @Autowired private ZookeeperServiceDiscovery serviceDiscovery;

    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent applicationEvent) {
        try {
            int servicePort = applicationEvent.getWebServer().getPort();
            String serviceHost = InetAddress.getLocalHost().getHostAddress();

            serviceDiscovery.registerService(
                    serviceHost,
                    servicePort,
                    serviceName,
                    VerificationController.CONTEXT + VerificationController.SERVICE,
                    serviceDescription
            );
        }
        catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(VerificationApplication.class, args);
    }
}
