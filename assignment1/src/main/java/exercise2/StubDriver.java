package exercise2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
            System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
            System.exit(-1);
        }

        /*
         * Instantiate a Job object for your job's configuration.
         */
        Job job = new Job();

        /*
         * Specify the jar file that contains your driver, mapper, and reducer.
         * Hadoop will transfer this jar file to nodes in your cluster running
         * mapper and reducer tasks.
         */
        job.setJarByClass(StubDriver.class);

        /*
         * Specify an easily-decipherable name for the job.
         * This job name will appear in reports and logs.
         */
        job.setJobName("Stub Driver");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(StubMapper.class);
        job.setReducerClass(StubReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));


        /*
         * Start the MapReduce job and wait for it to finish.
         * If it finishes successfully, return 0. If not, return 1.
         */
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

