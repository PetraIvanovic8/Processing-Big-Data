import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final String[] SEARCH_WORDS = {"hackathon", "dec", "chicago", "java", "engineers"};

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase(); // Convert the line to lowercase

        for (String word : SEARCH_WORDS) {
            if (line.contains(word)) {
                // Output the matched word as a key with a count of 1
                context.write(new Text(word), new IntWritable(1));
            }
            else {
                context.write(new Text(word), new IntWritable(0));
            }
        }
    }
}
