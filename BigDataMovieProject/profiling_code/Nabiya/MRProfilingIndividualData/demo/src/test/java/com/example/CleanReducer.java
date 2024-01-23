import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.eclipse.jetty.util.IO;

public class CleanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private final Map<String, Integer> map = new HashMap<>();

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String[] words = conf.get("input").toLowerCase().split(",");

    for (String word : words) {
      map.putIfAbsent(word, 0);
    }
  }

  @Override
  protected void cleanup(Context context)
    throws IOException, InterruptedException {
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      context.write(
        new Text(entry.getKey()),
        new IntWritable(entry.getValue())
      );
    }
  }

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
    int sum = 0;
    if (map.containsKey(key.toString())) {
      for (IntWritable value : values) {
        sum += value.get();
      }
      map.put(key.toString(), sum);
    }
  }
}


// int sum = 0;
//     for (IntWritable value : values) {
//       sum += value.get();
//     }
//     context.write(key, new IntWritable(sum));