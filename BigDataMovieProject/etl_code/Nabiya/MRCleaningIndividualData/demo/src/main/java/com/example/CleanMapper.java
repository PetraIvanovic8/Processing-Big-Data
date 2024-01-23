package com.example;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
  extends Mapper<LongWritable, Text, Text, NullWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    if (key.get() == 0 && value.toString().contains("title")) {
      return;
    }

    String[] columns = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

    if (columns.length > 3) {
      String title = columns[1];
   
      if (!title.startsWith("\"")) {
        title = title.contains(",") ? "\"" + title + "\"" : title;
      }

      String col = title + "," + columns[2] + "," + columns[3];
      context.write(new Text(col), NullWritable.get());
    }
}
}