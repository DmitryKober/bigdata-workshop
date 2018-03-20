package com.epam.bdcc.workshop.structurer.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static com.epam.bdcc.workshop.structurer.reducer.StructurerReducer.NULL_KEY;
import static com.epam.bdcc.workshop.structurer.reducer.StructurerReducer.SEPARATOR_FIELD;

/**
 * Created by Dmitrii_Kober on 6/9/2017.
 */
public class StructurerReducerTest {

    private ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;

    @Before
    public void setUp() {
        StructurerReducer reducer = new StructurerReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testDatabaseReducer() throws IOException {
        reduceDriver.withInput(new Text("[database] my-process-id"), Arrays.asList(new Text("[database] 2018-03-20 13:28:15.185 | Dmitrii | my-process-id | putSize: 546; returnSize: 1209")));
        StringBuilder expectedHiveRow = new StringBuilder();
        expectedHiveRow.append("2018-03-20 13:28:15.185").append(SEPARATOR_FIELD);
        expectedHiveRow.append("Dmitrii").append(SEPARATOR_FIELD);
        expectedHiveRow.append("my-process-id").append(SEPARATOR_FIELD);
        expectedHiveRow.append("546").append(SEPARATOR_FIELD);
        expectedHiveRow.append("1209").append(SEPARATOR_FIELD);

        reduceDriver.withOutput(NULL_KEY, new Text(expectedHiveRow.toString()));
        reduceDriver.runTest();
    }

    @Test
    public void testGeneratorReducer() throws IOException {
        reduceDriver.withInput(new Text("[generator] my-process-id"), Arrays.asList(new Text("[generator] (2018-03-20 13:28:53.715, 2018-03-20 13:29:01.797) | Dmitrii | my-process-id | avg cpu time: 0.1623209276141473")));
        StringBuilder expectedHiveRow = new StringBuilder();
        expectedHiveRow.append("2018-03-20 13:28:53.715").append(SEPARATOR_FIELD);
        expectedHiveRow.append("2018-03-20 13:29:01.797").append(SEPARATOR_FIELD);
        expectedHiveRow.append("Dmitrii").append(SEPARATOR_FIELD);
        expectedHiveRow.append("my-process-id").append(SEPARATOR_FIELD);
        expectedHiveRow.append("0.1623209276141473").append(SEPARATOR_FIELD);

        reduceDriver.withOutput(NULL_KEY, new Text(expectedHiveRow.toString()));
        reduceDriver.runTest();
    }

    @Test
    public void testVerificationReducer() throws IOException {
        reduceDriver.withInput(new Text("[verification] my-process-id"), Arrays.asList(new Text("[verification] 2018-03-20 13:29:41.909 | Dmitrii | my-process-id | records verified: 26118")));
        StringBuilder expectedHiveRow = new StringBuilder();
        expectedHiveRow.append("2018-03-20 13:29:41.909").append(SEPARATOR_FIELD);
        expectedHiveRow.append("Dmitrii").append(SEPARATOR_FIELD);
        expectedHiveRow.append("my-process-id").append(SEPARATOR_FIELD);
        expectedHiveRow.append("26118").append(SEPARATOR_FIELD);

        reduceDriver.withOutput(NULL_KEY, new Text(expectedHiveRow.toString()));
        reduceDriver.runTest();
    }
}
