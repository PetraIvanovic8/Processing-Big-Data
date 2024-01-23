package com.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueRecsMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    // Skip the header row
    if (key.get() == 0) {
      return;
    }

    String line = value.toString();
    context.write(new Text(line), new IntWritable(1));
  }
}
