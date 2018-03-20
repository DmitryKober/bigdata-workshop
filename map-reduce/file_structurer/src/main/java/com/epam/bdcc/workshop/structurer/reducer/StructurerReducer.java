package com.epam.bdcc.workshop.structurer.reducer;

import com.epam.bdcc.workshop.structurer.model.ServiceType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StructurerReducer extends Reducer<Text, Text, NullWritable, Text> {

    public static final String SEPARATOR_FIELD = new String(new char[] {1});
    public static final NullWritable NULL_KEY = NullWritable.get();

    private static final Logger LOG = LoggerFactory.getLogger(StructurerReducer.class);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder hiveRow = new StringBuilder();

        if (key.toString().contains("[" + ServiceType.DATABASE.value() + "]")) {
            for (Text value : values) {
                String valueStr = value.toString().trim();
                Matcher matcher = Pattern.compile("\\[.+\\] (.+) \\| (.+) \\| (.+) \\| putSize: (.+); returnSize: (.+)").matcher(valueStr);
                if ( ! matcher.find() ) {
                    LOG.warn("Unexpected database-service's line met: '{}'. Skipping.", valueStr);
                    return;
                }

                String timestamp = matcher.group(1);
                String userId = matcher.group(2);
                String workflowId = matcher.group(3);
                String putSize = matcher.group(4);
                String returnSize = matcher.group(5);

                hiveRow.append(timestamp).append(SEPARATOR_FIELD);
                hiveRow.append(userId).append(SEPARATOR_FIELD);
                hiveRow.append(workflowId).append(SEPARATOR_FIELD);
                hiveRow.append(putSize).append(SEPARATOR_FIELD);
                hiveRow.append(returnSize).append(SEPARATOR_FIELD);
            }

        }
        else if (key.toString().contains("[" + ServiceType.GENERATOR.value() + "]")) {
            for (Text value : values) {
                String valueStr = value.toString().trim();
                Matcher matcher = Pattern.compile("\\[.+\\] \\((.+), (.+)\\) \\| (.+) \\| (.+) \\| avg cpu time: (.+)").matcher(valueStr);
                if (!matcher.find()) {
                    LOG.warn("Unexpected generator-service's line met: '{}'. Skipping.", valueStr);
                    return;
                }

                String timestampFrom = matcher.group(1);
                String timestampTill = matcher.group(2);
                String userId = matcher.group(3);
                String workflowId = matcher.group(4);
                String avgCpuTime = matcher.group(5);

                hiveRow.append(timestampFrom).append(SEPARATOR_FIELD);
                hiveRow.append(timestampTill).append(SEPARATOR_FIELD);
                hiveRow.append(userId).append(SEPARATOR_FIELD);
                hiveRow.append(workflowId).append(SEPARATOR_FIELD);
                hiveRow.append(avgCpuTime).append(SEPARATOR_FIELD);
            }
        }
        else if (key.toString().contains("[" + ServiceType.VERIFICATION.value() + "]")) {
            for (Text value : values) {
                String valueStr = value.toString().trim();
                Matcher matcher = Pattern.compile("\\[.+\\] (.+) \\| (.+) \\| (.+) \\| records verified: (.+)").matcher(valueStr);
                if (!matcher.find()) {
                    LOG.warn("Unexpected verification-service's line met: '{}'. Skipping.", valueStr);
                    return;
                }

                String timestamp = matcher.group(1);
                String userId = matcher.group(2);
                String workflowId = matcher.group(3);
                String recordsVerified = matcher.group(4);

                hiveRow.append(timestamp).append(SEPARATOR_FIELD);
                hiveRow.append(userId).append(SEPARATOR_FIELD);
                hiveRow.append(workflowId).append(SEPARATOR_FIELD);
                hiveRow.append(recordsVerified).append(SEPARATOR_FIELD);
            }
        }

        context.write(NULL_KEY, new Text(hiveRow.toString()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
