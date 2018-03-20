package com.epam.bdcc.workshop.structurer.reducer;

import org.apache.hadoop.io.NullWritable;

/**
 * Created by Dmitrii_Kober on 3/20/2018.
 */
abstract class AbstractRecordProducer {

    public static final String SEPARATOR_FIELD = new String(new char[] {1});
    public static final NullWritable NULL_KEY = NullWritable.get();
}
