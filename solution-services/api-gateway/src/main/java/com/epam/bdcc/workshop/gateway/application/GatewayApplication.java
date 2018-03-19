package com.epam.bdcc.workshop.gateway.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Created by Dmitrii_Kober on 3/13/2018.
 */
@SpringBootApplication
@ComponentScan("com.epam.bdcc.workshop")
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
