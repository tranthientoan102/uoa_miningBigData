package exercise3;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SuggestReduce extends Reducer<LongWritable, SuggestWritable, LongWritable, Text> {
    @Override
    public void reduce(LongWritable fromId, Iterable<SuggestWritable> allSuggests, Context context)
            throws IOException, InterruptedException {

        List<SuggestWritable> allSuggestList = new ArrayList<>();
        List<Long> friendList = new ArrayList<>();

        // update friend list when mutual id is -1
        // otherwise, collect as suggest list
        for (SuggestWritable suggest : allSuggests){
            if (suggest.mutualId ==-1L){
                friendList.add(suggest.friendId);
//                friendList.sort(Long::compareTo);
            }else allSuggestList.add(new SuggestWritable(suggest));
        }
        // remove suggestion for existed friends
        allSuggestList = allSuggestList.stream().filter(x -> !friendList.contains(x.friendId))
                .collect(Collectors.toList());

        // summarize suggest list by grouping friends suggestion
        // ex: 2 suggestion [5 via 2] and [5 via 3] will be combined to [5 via [2,3]]
        Map<Long, List<Long>> sumSuggest = new HashMap<>();
        for (SuggestWritable suggest : allSuggestList){
            if (!sumSuggest.containsKey(suggest.friendId)){
                sumSuggest.put(suggest.friendId, new ArrayList<Long>());
            }
            sumSuggest.get(suggest.friendId).add(suggest.mutualId);

        }

        // implement sorting algorithm
        SortedMap<Long, List<Long>> sortSumSuggest = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long a, Long b) {
                int result = 0;
                // suggestion with more mutual friend have higher rank
                if (sumSuggest.get(a).size() > sumSuggest.get(b).size()) result = -1;
                // suggestion with less mutual friend have lower rank
                else if (sumSuggest.get(a).size() < sumSuggest.get(b).size()) result = 1;
                // in case of same number of mutual friends, smaller id have higher rank
                else result = a.compareTo(b);
                return result;
            }
        });

        // insert suggestion per user
        for (Map.Entry<Long, List<Long>> e: sumSuggest.entrySet()){
                sortSumSuggest.put(e.getKey(), e.getValue());
        }

        StringBuilder builder = new StringBuilder();
        int suggestCount = 0;
        for (Map.Entry<Long, List<Long>> e: sortSumSuggest.entrySet()){
            builder.append(String.format("%d%s,",e.getKey(), e.getValue().toString()));
            if (suggestCount++ == 9) break;
        }
        String output = builder.toString();
        int lastCommaIndex = output.lastIndexOf(',');
        output = (lastCommaIndex > 0)? output.substring(0, lastCommaIndex):output;
        context.write(fromId,new Text(output));




    }
}