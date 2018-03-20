package com.epam.bdcc.workshop.structurer.partitioner;

import com.epam.bdcc.workshop.structurer.model.ServiceType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
public class ServiceTypePartitioner extends Partitioner<Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceTypePartitioner.class);

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }

        String keyStr = key.toString();

        ServiceType serviceType = ServiceType.fromValue(keyStr);
        switch (serviceType) {
            case DATABASE: {
                return 0;
            }
            case GENERATOR: {
                return 1 % numPartitions;
            }
            case VERIFICATION: {
                return 2 % numPartitions;
            }
            default: {
                LOG.warn("Unknown service type spotted: '{}'. Skipping...", keyStr);
                return 0;
            }
        }
    }
}
