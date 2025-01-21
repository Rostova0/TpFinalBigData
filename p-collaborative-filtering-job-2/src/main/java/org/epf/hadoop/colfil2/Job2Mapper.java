package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Job2Mapper extends Mapper<LongWritable, Text, UserPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\\t");
        if (line.length < 2) return;

        String user = line[0];
        String[] friends = line[1].split(",");

        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                UserPair pair = new UserPair(friends[i], friends[j]);
                context.write(pair, new Text(user));
            }
        }
    }
}
