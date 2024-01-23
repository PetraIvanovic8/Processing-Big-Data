
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper 
extends Mapper<LongWritable, Text, Text, IntWritable> 
{

  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    String line = value.toString();
    String[] titles = line.split("[-\\s,:]+");
    Set<String> titleSet = new HashSet<>(Arrays.asList(titles));
   
    for(String title: titleSet){
      context.write(new Text(title), new IntWritable(1));
    }
  }
}
