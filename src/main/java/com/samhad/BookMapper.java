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
                StringBuilder stringBuilder = new StringBuilder(datum[2]);

                for (int i = 3; i < datum.length; i++) {
                    if (datum[i].trim().matches("\\d+.\\d+")) {
                        break;
                    } else {
                        foundPos++;
                        stringBuilder.append(",");
                        stringBuilder.append(datum[i]);
                    }
                }

                String val = datum[1] + "\trating " + datum[3 + foundPos];
                keyText.set(stringBuilder.toString());
                valueText.set(val);
                context.write(keyText, valueText);
            }
        }
    }
}
