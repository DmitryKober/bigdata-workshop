package com.epam.bdcc.workshop.gateway.kafka;

import com.epam.bdcc.workshop.TestUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;

/**
 * Created by Dmitrii_Kober on 3/15/2018.
 */
public class EmbeddedKafkaCluster {
    private static String zkConnection;
    private static int port;
    private static KafkaServerStartable broker;
    private static File logDir;
    private static EmbeddedKafkaCluster instance;

    public static EmbeddedKafkaCluster instance(String instanceZkConnection, int instancePort) {
        if (instance == null) {
            instance = new EmbeddedKafkaCluster();
            zkConnection = instanceZkConnection;
            port = instancePort;
            logDir = TestUtils.constructTempDir("kafka-local");
            return instance;
        }

        if (zkConnection.equals(instanceZkConnection) && port == instancePort && instance != null) {
            return instance;
        }


        throw new RuntimeException("A Kafka broker instance is already registered on port '" + port + "' and zkConnection '" + zkConnection + "'.");
    }

    public static EmbeddedKafkaCluster instance() {
        if (instance != null) {
            return instance;
        }

        throw new RuntimeException("No Kafka broker instance is registered.");
    }

    public void startup() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", zkConnection);
        properties.setProperty("broker.id", "1");
        properties.setProperty("zookeeper.connection.timeout.ms", "10000");
        properties.setProperty("zookeeper.session.timeout.ms", "10000");
        properties.setProperty("host.name", "localhost");
        properties.setProperty("port", port + "");
        properties.setProperty("log.dir", logDir.getAbsolutePath());
        properties.setProperty("log.flush.interval.messages", String.valueOf(1));

        broker = new KafkaServerStartable(new KafkaConfig(properties));
        broker.startup();
    }

    public void shutdown() {
        try {
            broker.shutdown();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            TestUtils.deleteFile(logDir);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
