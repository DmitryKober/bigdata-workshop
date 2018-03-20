package com.epam.bdcc.workshop.structurer.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.epam.bdcc.workshop.structurer.model.ServiceType.GENERATOR;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
class GeneratorServiceKeyProducer {

    private static final Pattern PATTERN = Pattern.compile("\\[.+\\] \\(.+\\) \\| .+ \\| (.+) \\| .+");
    private static Logger LOG = LoggerFactory.getLogger(GeneratorServiceKeyProducer.class);

    static Optional<String> handle(String source) {
        Matcher generatorMatcher = PATTERN.matcher(source);
        if (generatorMatcher.find()) {
            return Optional.of("[" + GENERATOR.value() + "] " + generatorMatcher.group(1));
        }

        LOG.warn("Unexpected format met for the 'generator' service: '{}'. Skipping", source);
        return Optional.empty();
    }
}
