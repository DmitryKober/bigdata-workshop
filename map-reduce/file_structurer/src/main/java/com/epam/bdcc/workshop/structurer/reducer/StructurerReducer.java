package com.epam.bdcc.workshop.structurer.reducer;

import com.epam.bdcc.workshop.structurer.model.ServiceType;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class StructurerReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(StructurerReducer.class);



    private MultipleOutputs<NullWritable, Text> mos;
    private Text rowText = new Text();

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        if (key.toString().contains("[" + ServiceType.DATABASE.value() + "]")) {
            for (Text value : values) {
                Optional<String> hiveRow = DatabaseResultRecordProducer.handle(value.toString());
                hiveRow.ifPresent(hiveRowStr -> {
                    rowText.set(hiveRowStr);
                    try {
                        mos.write(NullWritable.get(), rowText, ServiceType.DATABASE.value() + "/stats");
                    }
                    catch (IOException | InterruptedException e) {
                        LOG.error("An error raised in the reducer:", e);
                    }
                });
            }

        }
        else if (key.toString().contains("[" + ServiceType.GENERATOR.value() + "]")) {
            for (Text value : values) {
                Optional<String> hiveRow = GeneratorResultRecordProducer.handle(value.toString());
                hiveRow.ifPresent(hiveRowStr -> {
                    rowText.set(hiveRowStr);
                    try {
                        mos.write(NullWritable.get(), rowText, ServiceType.GENERATOR.value() + "/stats");
                    }
                    catch (IOException | InterruptedException e) {
                            LOG.error("An error raised in the reducer:", e);
                        }
                    });
            }
        }
        else if (key.toString().contains("[" + ServiceType.VERIFICATION.value() + "]")) {
            for (Text value : values) {
                Optional<String> hiveRow = VerificationResultRecordProducer.handle(value.toString());
                hiveRow.ifPresent(hiveRowStr -> {
                    rowText.set(hiveRowStr);
                    try {
                        mos.write(NullWritable.get(), rowText, ServiceType.VERIFICATION.value()+ "/stats");
                    }
                    catch (IOException | InterruptedException e) {
                        LOG.error("An error raised in the reducer:", e);
                    }
                });
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
