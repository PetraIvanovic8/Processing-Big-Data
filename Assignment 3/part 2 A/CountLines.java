import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountLines {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountLines <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(CountLines.class);
        job.setJobName("CountLines");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CountLinesMapper.class);
        job.setReducerClass(CountLinesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class CountLinesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final static Text countKey = new Text("Total number of lines in AirBNB file:");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // For each line in the input, output a key-value pair with the countKey and 1 as the value.
            context.write(countKey, one);
        }
    }


    public static class CountLinesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalCount = 0;

            // Sum up the counts to get the total number of lines
            for (IntWritable value : values) {
                totalCount += value.get();
            }

            // Output the total number of lines
            context.write(key, new IntWritable(totalCount));
        }
    }
   
}
