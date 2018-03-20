package com.epam.bdcc.workshop.structurer.reducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
class GeneratorResultRecordProducer extends AbstractRecordProducer {

    private static final Pattern PATTERN = Pattern.compile("\\[.+\\] \\((.+), (.+)\\) \\| (.+) \\| (.+) \\| avg cpu time: (.+)");
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorResultRecordProducer.class);

    static Optional<String> handle(String source) {
        String trimmedStr = source.trim();
        Matcher matcher = PATTERN.matcher(trimmedStr);
        if ( ! matcher.find() ) {
            LOG.warn("Unexpected generator-service's line met: '{}'. Skipping.", trimmedStr);
            return Optional.empty();
        }

        String timestampFrom = matcher.group(1);
        String timestampTill = matcher.group(2);
        String userId = matcher.group(3);
        String workflowId = matcher.group(4);
        String avgCpuTime = matcher.group(5);

        StringBuilder resultRecord = new StringBuilder();
        resultRecord.append(timestampFrom).append(SEPARATOR_FIELD);
        resultRecord.append(timestampTill).append(SEPARATOR_FIELD);
        resultRecord.append(userId).append(SEPARATOR_FIELD);
        resultRecord.append(workflowId).append(SEPARATOR_FIELD);
        resultRecord.append(avgCpuTime).append(SEPARATOR_FIELD);

        return Optional.of(resultRecord.toString());
    }
}
