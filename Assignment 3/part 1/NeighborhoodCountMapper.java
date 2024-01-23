import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NeighborhoodCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        if (fields.length == 16) { //eliminate all rows with too many fields (extra ,) or too few fields
            String neighborhoodGroup = fields[4]; // Neighborhood Group is in column 5
            String neighborhood = fields[5]; // Neighborhood is in column 6

            // Combine neighborhood group and neighborhood as the key and output 1 as the value
            context.write(new Text(neighborhoodGroup + " " + neighborhood), new IntWritable(1));
        }
    }
}