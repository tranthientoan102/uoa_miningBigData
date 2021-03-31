package exercise3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendSuggestion extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
//        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new FriendSuggestion(), args);

        System.exit(res);
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "FriendSuggestion");
        job.setJarByClass(FriendSuggestion.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(SuggestWritable.class);

        job.setMapperClass(SuggestMapper.class);
        job.setReducerClass(SuggestReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        if (args.length ==2) {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        }
        else if (args.length ==3) {
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
        }
        else {
//            too many arguments
            System.exit(1);
        }
        job.waitForCompletion(true);

        return 0;
    }
}
