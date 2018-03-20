package com.epam.bdcc.workshop.structurer.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.epam.bdcc.workshop.structurer.model.ServiceType.DATABASE;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
class DatabaseServiceKeyProducer {

    private static Pattern PATTERN = Pattern.compile("\\[.+\\] .+ \\| .+ \\| (.+) \\| .+");
    private static Logger LOG = LoggerFactory.getLogger(DatabaseServiceKeyProducer.class);

    static Optional<String> handle(String source) {
        Matcher databaseMatcher = PATTERN.matcher(source);
        if (databaseMatcher.find()) {
            return Optional.of("[" + DATABASE.value() + "] " + databaseMatcher.group(1));
        }

        LOG.warn("Unexpected format met for the 'database' service: '{}'. Skipping", source);
        return Optional.empty();
    }
}
