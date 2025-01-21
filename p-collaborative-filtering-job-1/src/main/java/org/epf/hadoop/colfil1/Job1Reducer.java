package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> uniqueFriends = new HashSet<>();

        for (Text value : values) {
            uniqueFriends.add(value.toString());
        }

        String joinedFriends = String.join(",", uniqueFriends);

        result.set(joinedFriends);
        context.write(key, result);
    }
}
