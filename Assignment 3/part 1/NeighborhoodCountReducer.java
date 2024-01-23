import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NeighborhoodCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts for each neighborhood or neighborhood group
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Output the neighborhood or neighborhood group and its total count
        context.write(key, new IntWritable(sum));
    }
}