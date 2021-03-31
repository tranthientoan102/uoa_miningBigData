package p3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static IntWritable ONE = new IntWritable(1);
//    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        for (String token: value.toString().split("[!\\\"#$%&'()*+,\\-./:;<=>?@\\[\\]^_`{|}~\\s]+")) {
//            word.set(token);
            context.write(new Text(token), new Text(""));
        }

    }
}
