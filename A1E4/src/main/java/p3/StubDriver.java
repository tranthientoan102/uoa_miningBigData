package p3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import p1.LengthMapper;
import p1.LengthReducer;

public class StubDriver {

    public static void main(String[] args) throws Exception {

        String inputPath = "";
        String outPath = "";

        /*
         * Validate that two or three arguments were passed
         */
        if (args.length == 2){
            inputPath = args[0];
            outPath = args[1];
        }else if (args.length == 3){
            inputPath = args[1];
            outPath = args[2];
        }else {
            System.out.print("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }
        String outPathJob1 = outPath + "/job1";
        String outPathJob2 = outPath + "/job2";

        /*
         * Job 1
         */
        Configuration conf = new Configuration();
        Job job1 = new Job(conf, "word filter");

        job1.setJarByClass(StubDriver.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(WordReducer.class);

//        job1.setInputFormatClass(TextInputFormat.class);
//        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outPathJob1));

        job1.waitForCompletion(true);
//        boolean success = job2.waitForCompletion(true);

        /*
         * Job 1
         */
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf, "word filter");

        job2.setJarByClass(StubDriver.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(LengthMapper.class);
        job2.setReducerClass(LengthReducer.class);

//        job2.setInputFormatClass(TextInputFormat.class);
//        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(outPathJob1 + "/part*"));
        FileOutputFormat.setOutputPath(job2, new Path(outPathJob2));





        System.exit(job2.waitForCompletion(true) ? 0 : 1);



    }
}

