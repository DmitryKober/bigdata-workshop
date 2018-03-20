package com.epam.bdcc.workshop.structurer.reducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
class VerificationResultRecordProducer extends AbstractRecordProducer {

    private static final Pattern PATTERN = Pattern.compile("\\[.+\\] (.+) \\| (.+) \\| (.+) \\| records verified: (.+)");
    private static final Logger LOG = LoggerFactory.getLogger(VerificationResultRecordProducer.class);

    static Optional<String> handle(String source) {
        String trimmedStr = source.trim();
        Matcher matcher = PATTERN.matcher(trimmedStr);
        if ( ! matcher.find() ) {
            LOG.warn("Unexpected verification-service's line met: '{}'. Skipping.", trimmedStr);
            return Optional.empty();
        }

        String timestamp = matcher.group(1);
        String userId = matcher.group(2);
        String workflowId = matcher.group(3);
        String recordsVerified = matcher.group(4);

        StringBuilder resultRecord = new StringBuilder();
        resultRecord.append(timestamp).append(SEPARATOR_FIELD);
        resultRecord.append(userId).append(SEPARATOR_FIELD);
        resultRecord.append(workflowId).append(SEPARATOR_FIELD);
        resultRecord.append(recordsVerified).append(SEPARATOR_FIELD);

        return Optional.of(resultRecord.toString());
    }
}
