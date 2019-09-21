package com.samhad;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class BookReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iterator = values.iterator();

        while (iterator.hasNext()) {

            context.write(key, iterator.next());
        }
    }
}
