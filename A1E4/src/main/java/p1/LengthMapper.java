package p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LengthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        for (String token: value.toString().split("\\s+")) {
            context.write(new IntWritable(token.length()), ONE);
        }
    }
}