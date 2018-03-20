package com.epam.bdcc.workshop.structurer.mapper;

import com.epam.bdcc.workshop.structurer.model.ServiceType;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Dmitrii_Kober on 6/9/2017.
 */
public class StructurerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(StructurerMapper.class);
    private static final Pattern PATTERN = Pattern.compile("\\[(.+)\\]");

    private Text mapKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String textLine = value.toString();

        Matcher matcher = PATTERN.matcher(textLine);

        if ( ! matcher.find() ) {
            LOG.warn("Unexpected line met: '{}'. Skipping...", textLine);
            return;
        }

        ServiceType serviceType = ServiceType.fromValue(matcher.group(1));

        Optional<String> mapKeyOpt;
        switch (serviceType) {
            case DATABASE: {
                mapKeyOpt = DatabaseServiceKeyProducer.handle(textLine);
                break;
            }
            case GENERATOR: {
                mapKeyOpt = GeneratorServiceKeyProducer.handle(textLine);
                break;
            }
            case VERIFICATION: {
                mapKeyOpt = VerificationServiceKeyProducer.handle(textLine);
                break;
            }
            default: {
                LOG.warn("Unexpected format met: '{}'. Skipping", textLine);
                return;
            }
        }

        mapKeyOpt.ifPresent(mapKeyStr -> {
            mapKey.set(mapKeyStr);
            try {
                context.write(mapKey, value);
            }
            catch (InterruptedException | IOException e) {
                LOG.error("An error raised in the mapper:", e);
            }
        });
    }
}
