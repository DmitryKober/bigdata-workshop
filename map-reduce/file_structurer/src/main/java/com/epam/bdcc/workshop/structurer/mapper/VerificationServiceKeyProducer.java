package com.epam.bdcc.workshop.structurer.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.epam.bdcc.workshop.structurer.model.ServiceType.VERIFICATION;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
class VerificationServiceKeyProducer {

    private static final Pattern PATTERN = Pattern.compile("\\[.+\\] .+ \\| .+ \\| (.+) \\| .+");
    private static final Logger LOG = LoggerFactory.getLogger(VerificationServiceKeyProducer.class);

    static Optional<String> handle(String source) {
        Matcher verificationMatcher = PATTERN.matcher(source);
        if (verificationMatcher.find()) {
            return Optional.of("[" + VERIFICATION.value() + "] " + verificationMatcher.group(1));
        }

        LOG.warn("Unexpected format met for the 'verification' service: '{}'. Skipping", source);
        return Optional.empty();
    }
}
