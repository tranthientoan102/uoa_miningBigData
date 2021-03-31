package exercise3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class SuggestMapper extends Mapper<LongWritable, Text, LongWritable, SuggestWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        String[] data = value.toString().split("\\t");
        if (data.length ==2) {
            // only process if a user have friend
            String[] ids_string = data[1].split(",");

            long fromId = Long.parseLong(data[0]);
            long[] ids_long = parseIds(ids_string);

            updateContext(fromId, ids_long, context);
        }
    }
    public long[] parseIds(String[] inputId){
        long[] result = new long[inputId.length];
        for (int i = 0; i < inputId.length; i++){
            result[i] = Long.parseLong(inputId[i]);

        }
        Arrays.sort(result);
        return result;
    }
    public void updateContext(long fromId, long[] friendIds, Context context) throws IOException, InterruptedException {

        // update suggestions
        for (int i= 0; i < friendIds.length; i++){
            for (int j = 0; j < friendIds.length; j++){
                if (j != i) context.write(new LongWritable(friendIds[i]), new SuggestWritable(friendIds[j], fromId));
            }
        }

        // add existing friendship
        for (long id : friendIds){
            context.write(new LongWritable(fromId), new SuggestWritable(id, -1L));
        }
    }
}
