package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job1Mapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text userKey = new Text();
    private Text friendValue = new Text();

    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        String user1 = value.getId1();
        String user2WithTimestamp = value.getId2();

        String[] user2Parts = user2WithTimestamp.split(",");
        String user2 = user2Parts[0].trim();

        userKey.set(user1);
        friendValue.set(user2);
        context.write(userKey, friendValue);

        userKey.set(user2);
        friendValue.set(user1);
        context.write(userKey, friendValue);
    }
}
