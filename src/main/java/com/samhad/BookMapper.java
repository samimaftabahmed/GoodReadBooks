package com.samhad;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class BookMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text keyText = new Text();
        Text valueText = new Text();
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");

        while (tokenizer.hasMoreElements()) {

            String token = tokenizer.nextToken();

            if (!token.equals("bookID,title,authors,average_rating,isbn,isbn13,language_code,# num_pages,ratings_count,text_reviews_count")) {

                String[] datum = token.split(",");

                int foundPos = 0;
                StringBuilder stringBuilder = new StringBuilder(datum[2].trim());

                for (int i = 3; i < datum.length; i++) {
                    if (datum[i].trim().matches("\\d+.\\d+")) {
                        context.getCounter(RECORD_TYPES.GOOD_RECORDS).increment(1);
                        break;
                    } else {
                        foundPos++;
                        context.getCounter(RECORD_TYPES.JUNK_RECORDS).increment(1);
                        stringBuilder.append(",");
                        stringBuilder.append(datum[i].trim());
                    }
                }

                String val = datum[1].trim() + "\trating " + datum[3 + foundPos].trim();
                keyText.set(stringBuilder.toString());
                valueText.set(val);
                context.write(keyText, valueText);
            }
        }
    }

    private enum RECORD_TYPES {
        GOOD_RECORDS,
        JUNK_RECORDS
    }
}
