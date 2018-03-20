package com.epam.bdcc.workshop.structurer.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Dmitrii_Kober on 6/9/2017.
 */
public class StructurerMapperTest {

    private MapDriver<LongWritable, Text, Text, Text> mapDriver;

    @Before
    public void setUp() {
        StructurerMapper mapper = new StructurerMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testDatabaseRowMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("[database] 2018-03-20 13:28:15.185 | Dmitrii | my-process-id | putSize: 546; returnSize: 1209"));
        mapDriver.withOutput(new Text("[database] my-process-id"), new Text("[database] 2018-03-20 13:28:15.185 | Dmitrii | my-process-id | putSize: 546; returnSize: 1209"));
        mapDriver.runTest();
    }

    @Test
    public void testGeneratorRowMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("[generator] (2018-03-20 13:28:53.715, 2018-03-20 13:29:01.797) | Dmitrii | my-process-id | avg cpu time: 0.1623209276141473"));
        mapDriver.withOutput(new Text("[generator] my-process-id"), new Text("[generator] (2018-03-20 13:28:53.715, 2018-03-20 13:29:01.797) | Dmitrii | my-process-id | avg cpu time: 0.1623209276141473"));
        mapDriver.runTest();
    }

    @Test
    public void testVerificationRowMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("[verification] 2018-03-20 13:29:41.909 | Dmitrii | my-process-id | records verified: 26118"));
        mapDriver.withOutput(new Text("[verification] my-process-id"), new Text("[verification] 2018-03-20 13:29:41.909 | Dmitrii | my-process-id | records verified: 26118"));
        mapDriver.runTest();
    }
}
