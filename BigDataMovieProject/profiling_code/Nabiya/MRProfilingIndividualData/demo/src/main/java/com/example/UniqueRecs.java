package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class UniqueRecs {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: UniqueRecs-1.0 <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Use cleanboxoffice");
    job.setJarByClass(UniqueRecs.class);

    job.setMapperClass(UniqueRecsMapper.class);
    job.setReducerClass(UniqueRecsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
