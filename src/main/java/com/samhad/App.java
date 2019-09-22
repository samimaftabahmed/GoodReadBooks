package com.samhad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool {

    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new App(), args);
            System.exit(res);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf;
        Job job = Job.getInstance(this.getConf(), "Good Read Books Job");

        String[] otherArgs = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar GoodReadBooks.jar </input-path> </output-path>");
            return 2;
        }

        job.setJarByClass(App.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(BookMapper.class);
        job.setReducerClass(BookReducer.class);
        job.setCombinerClass(BookReducer.class);
        job.setPartitionerClass(RatingPartitioner.class);
        job.setNumReduceTasks(7);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
