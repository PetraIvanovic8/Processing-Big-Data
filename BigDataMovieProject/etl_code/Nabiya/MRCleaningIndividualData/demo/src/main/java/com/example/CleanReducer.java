package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer
  extends Reducer<Text, IntWritable, Text, NullWritable> {

  public boolean isReducerCalled = true;

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
    if (isReducerCalled) {
      isReducerCalled = false;
      return;
    }
    context.write(key, NullWritable.get());
  }

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    context.write(new Text("title,studio,lifetime_gross"), NullWritable.get());
  }
}
