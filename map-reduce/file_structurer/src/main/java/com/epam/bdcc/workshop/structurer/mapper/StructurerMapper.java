package com.epam.bdcc.workshop.structurer.mapper;

import com.epam.bdcc.workshop.structurer.model.ServiceType;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.epam.bdcc.workshop.structurer.model.ServiceType.DATABASE;
import static com.epam.bdcc.workshop.structurer.model.ServiceType.GENERATOR;
import static com.epam.bdcc.workshop.structurer.model.ServiceType.VERIFICATION;

/**
 * Created by Dmitrii_Kober on 6/9/2017.
 */
public class StructurerMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(StructurerMapper.class);

    private Text mapKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String textLine = value.toString();

        Matcher matcher = Pattern.compile("\\[(.+)\\]").matcher(textLine);

        if ( ! matcher.find() ) {
            LOG.warn("Unexpected line met: '{}'. Skipping...", textLine);
            return;
        }

        ServiceType serviceType = ServiceType.fromValue(matcher.group(1));

        String mapKeyStr = null;
        switch (serviceType) {
            case DATABASE: {
                Matcher databaseMatcher = Pattern.compile("\\[.+\\] .+ \\| .+ \\| (.+) \\| .+").matcher(textLine);
                if (databaseMatcher.find()) {
                    mapKeyStr = "[" + DATABASE.value() + "] " + databaseMatcher.group(1);
                }
                else {
                    LOG.warn("Unexpected format met for the 'database' service: '{}'. Skipping", textLine);
                    return;
                }
                break;
            }
            case GENERATOR: {
                // [generator] (2018-03-20 13:28:53.715, 2018-03-20 13:29:01.797) | Dmitrii | my-process-id | avg cpu time: 0.1623209276141473
                Matcher generatorMatcher = Pattern.compile("\\[.+\\] \\(.+\\) \\| .+ \\| (.+) \\| .+").matcher(textLine);
                if (generatorMatcher.find()) {
                    mapKeyStr = "[" + GENERATOR.value() + "] " + generatorMatcher.group(1);
                }
                else {
                    LOG.warn("Unexpected format met for the 'generator' service: '{}'. Skipping", textLine);
                    return;
                }
                break;
            }
            case VERIFICATION: {
                Matcher verificationMatcher = Pattern.compile("\\[.+\\] .+ \\| .+ \\| (.+) \\| .+").matcher(textLine);
                if (verificationMatcher.find()) {
                    mapKeyStr = "[" + VERIFICATION.value() + "] " + verificationMatcher.group(1);
                }
                else {
                    LOG.warn("Unexpected format met for the 'verification' service: '{}'. Skipping", textLine);
                    return;
                }
                break;
            }
            default: {
                LOG.warn("Unexpected format met: '{}'. Skipping", textLine);
                return;
            }
        }

        mapKey.set(mapKeyStr);

        try {
            context.write(mapKey, value);
        }
        catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
