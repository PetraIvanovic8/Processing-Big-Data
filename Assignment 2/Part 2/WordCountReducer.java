import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts for each word
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Output the word and its total count
        context.write(key, new IntWritable(sum));
    }
}
