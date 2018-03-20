package com.epam.bdcc.workshop.structurer.mapreduce;

import com.epam.bdcc.workshop.structurer.mapper.StructurerMapper;
import com.epam.bdcc.workshop.structurer.reducer.StructurerReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Dmitrii_Kober on 6/9/2017.
 */
public class StructurerMapReduceTest {

    private MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;

    @Before
    public void setUp() {
        StructurerMapper mapper = new StructurerMapper();
        StructurerReducer reducer = new StructurerReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
//      mapReduceDriver.withInput(new LongWritable(), new Text("a b c d ab cd ef abc def gtr abcd hbgn gtbd"));
//      mapReduceDriver.withOutput(new IntWritable(4), new Text("abcd, hbgn, gtbd"));
//      mapReduceDriver.runTest();
    }
}
