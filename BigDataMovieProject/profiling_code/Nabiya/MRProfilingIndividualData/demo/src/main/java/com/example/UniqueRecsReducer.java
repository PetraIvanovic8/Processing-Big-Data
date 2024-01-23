package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueRecsReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  int sum = 0;

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    context.write(
        new Text("Total lines: "),
        new IntWritable(sum));
  }

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    for (IntWritable value : values) {
      sum += value.get();
    }
    context.write(key, new IntWritable());
  }
}
