package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Job3Reducer extends Reducer<Text, Recommendation, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Recommendation> values, Context context) throws IOException, InterruptedException {
        List<Recommendation> recommendations = new ArrayList<>();
        for (Recommendation recommendation : values) {
            recommendations.add(new Recommendation(recommendation.getUserId(), recommendation.getCommonRelations()));
        }

        recommendations.sort(Comparator.comparingInt(Recommendation::getCommonRelations).reversed());

        StringBuilder sb = new StringBuilder();
        for (Recommendation recommendation : recommendations) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(recommendation.toString());
        }

        context.write(key, new Text(sb.toString()));
    }
}
